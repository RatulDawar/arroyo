use anyhow::Result;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::{ControlMessage, MetadataField};
use arroyo_state::global_table_config;
use arroyo_types::*;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use governor::{Quota, RateLimiter};
use iggy::client::{Client, MessageClient, TopicClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::models::messages::PolledMessage;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tracing::{error, info};

use crate::iggy::IggySourceOffset;

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct IggyState {
    offset: u64,
}

pub struct IggySourceFunc {
    pub stream: String,
    pub topic: String,
    pub partition_id: u32,
    pub consumer_id: u32,
    pub endpoint: String,
    pub transport: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub offset_mode: IggySourceOffset,
    pub auto_commit: bool,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub client: Option<IggyClient>,
    pub messages_per_second: NonZeroU32,
    pub metadata_fields: Vec<MetadataField>,
}

impl IggySourceFunc {
    async fn init_client(&mut self) -> Result<()> {
        info!("Creating Iggy client for {}", self.endpoint);

        // Create client with default configuration
        let client = IggyClient::default();

        // Set server address based on transport
        // For now, we'll just use the endpoint as is
        // The client will determine the appropriate protocol based on the URL format

        // Connect to the server
        client.connect().await?;

        // Authenticate if credentials are provided
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            client.login_user(username, password).await?;
        }

        self.client = Some(client);
        Ok(())
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        // Initialize the client if not already initialized
        if self.client.is_none() {
            self.init_client().await.map_err(|e| {
                UserError::new(
                    "Could not create Iggy client",
                    format!("Error: {:?}", e),
                )
            })?;
        }

        let rate_limiter = RateLimiter::direct(Quota::per_second(self.messages_per_second));

        // Initialize the deserializer
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &self.metadata_fields,
        );

        // Determine the starting offset
        let mut offset = match self.offset_mode {
            IggySourceOffset::Earliest => 0,
            IggySourceOffset::Latest => {
                // Get the last offset from the server
                let stream_id = Identifier::from_str(&self.stream).map_err(|e| {
                    UserError::new(
                        "Invalid stream identifier",
                        format!("Error: {:?}", e),
                    )
                })?;

                let topic_id = Identifier::from_str(&self.topic).map_err(|e| {
                    UserError::new(
                        "Invalid topic identifier",
                        format!("Error: {:?}", e),
                    )
                })?;

                // Get the highest offset for the partition
                let topic_info = self.client.as_ref().unwrap().get_topic(&stream_id, &topic_id).await.map_err(|e| {
                    UserError::new(
                        "Could not get topic info",
                        format!("Error: {:?}", e),
                    )
                })?;

                // Find the partition
                let partition = topic_info.partitions.iter()
                    .find(|p| p.id == self.partition_id)
                    .ok_or_else(|| {
                        UserError::new(
                            "Partition not found",
                            format!("Partition {} not found in topic {}", self.partition_id, self.topic),
                        )
                    })?;

                // Get the highest offset
                partition.current_offset
            }
        };

        // Create a consumer
        let consumer = Consumer::new(Identifier::numeric(self.consumer_id).unwrap());

        // Main polling loop
        loop {
            // Check for control messages
            if let Some(control_message) = ctx.control_rx.try_recv().ok() {
                match control_message {
                    ControlMessage::Checkpoint(barrier) => {
                        // Store the state before checkpointing
                        let state = IggyState { offset };
                        let table = ctx.table_manager.get_global_keyed_state("iggy").await
                            .map_err(|err| UserError::new("Failed to get global key value", err.to_string()))?;
                        table.insert(self.partition_id, state).await;

                        // Checkpoint without holding the client reference
                        let client_temp = self.client.take();
                        let result = self.start_checkpoint(barrier, ctx, collector).await;
                        self.client = client_temp;

                        if result {
                            return Ok(SourceFinishType::Immediate);
                        }
                    }
                    ControlMessage::Stop { mode } => {
                        info!("Received stop command with mode {:?}", mode);
                        match mode {
                            StopMode::Graceful => {
                                return Ok(SourceFinishType::Graceful);
                            }
                            StopMode::Immediate => {
                                return Ok(SourceFinishType::Immediate);
                            }
                        }
                    }
                    ControlMessage::LoadCompacted { compacted } => {
                        ctx.load_compacted(compacted).await;
                    }
                    ControlMessage::NoOp => {}
                    ControlMessage::Commit { .. } => {
                        unreachable!("Sources shouldn't receive commit messages");
                    }
                }
            }

            // Poll for messages
            let stream_id = Identifier::from_str(&self.stream).map_err(|e| {
                UserError::new(
                    "Invalid stream identifier",
                    format!("Error: {:?}", e),
                )
            })?;

            let topic_id = Identifier::from_str(&self.topic).map_err(|e| {
                UserError::new(
                    "Invalid topic identifier",
                    format!("Error: {:?}", e),
                )
            })?;

            let polling_strategy = PollingStrategy::offset(offset);

            let messages_per_batch = 100; // Configurable batch size

            let polled_messages = match self.client.as_ref().unwrap()
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    Some(self.partition_id),
                    &consumer,
                    &polling_strategy,
                    messages_per_batch,
                    self.auto_commit,
                )
                .await
            {
                Ok(messages) => messages,
                Err(e) => {
                    error!("Error polling messages: {:?}", e);
                    // Back off and retry
                    sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            };

            if polled_messages.messages.is_empty() {
                // No messages available, wait a bit before polling again
                sleep(Duration::from_millis(100)).await;
                continue;
            }

            // Process the messages
            for message in &polled_messages.messages {
                // Update the offset for the next poll
                offset = message.offset + 1;

                // Process the message
                self.process_message(message, collector).await?;

                // Apply rate limiting if needed
                if let Err(_) = rate_limiter.check() {
                    // Wait a bit before processing the next message
                    sleep(Duration::from_millis(10)).await;
                }
            }

            // Flush the buffer if needed
            if collector.should_flush() {
                collector.flush_buffer().await?;
            }
        }
    }

    async fn process_message(
        &self,
        message: &PolledMessage,
        collector: &mut SourceCollector,
    ) -> Result<(), UserError> {
        // Convert the message to the format expected by Arroyo
        let timestamp = {
            let nanos = message.timestamp as i64 * 1_000_000; // Convert to nanoseconds
            SystemTime::UNIX_EPOCH + Duration::from_nanos(nanos as u64)
        };

        // Add metadata fields if needed
        let mut additional_fields = HashMap::new();
        for field in &self.metadata_fields {
            match field.key.as_str() {
                "offset" => {
                    additional_fields.insert(field.field_name.as_str(), arroyo_formats::de::FieldValueType::Int64(Some(message.offset as i64)));
                }
                "partition" => {
                    additional_fields.insert(field.field_name.as_str(), arroyo_formats::de::FieldValueType::Int32(Some(self.partition_id as i32)));
                }
                "stream" => {
                    additional_fields.insert(field.field_name.as_str(), arroyo_formats::de::FieldValueType::String(Some(&self.stream)));
                }
                "topic" => {
                    additional_fields.insert(field.field_name.as_str(), arroyo_formats::de::FieldValueType::String(Some(&self.topic)));
                }
                "timestamp" => {
                    additional_fields.insert(field.field_name.as_str(), arroyo_formats::de::FieldValueType::Int64(Some(message.timestamp as i64)));
                }
                _ => {}
            }
        }

        // Deserialize the message
        collector
            .deserialize_slice(&message.payload, timestamp, if additional_fields.is_empty() { None } else { Some(&additional_fields) })
            .await?;

        Ok(())
    }
}

#[async_trait]
impl SourceOperator for IggySourceFunc {
    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        match self.run_int(ctx, collector).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;
                panic!("{}: {}", e.name, e.details);
            }
        }
    }

    fn name(&self) -> String {
        format!("iggy-{}-{}", self.stream, self.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("iggy", "iggy source state")
    }
}
