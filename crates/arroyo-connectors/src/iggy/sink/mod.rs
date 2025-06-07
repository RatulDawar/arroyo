use anyhow::Result;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::OperatorContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::*;
use async_trait::async_trait;
use bytes::Bytes;
use iggy::client::{Client, MessageClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning};
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::iggy::PartitioningStrategy;

pub struct IggySinkFunc {
    pub stream: String,
    pub topic: String,
    pub partitioning: PartitioningStrategy,
    pub endpoint: String,
    pub _transport: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client: Option<IggyClient>,
    pub serializer: ArrowSerializer,
}

impl IggySinkFunc {
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

    async fn send_messages(
        &mut self,
        messages: Vec<Message>,
        ctx: &mut OperatorContext,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        // Ensure client is initialized
        if self.client.is_none() {
            self.init_client().await?;
        }

        let client = self.client.as_ref().unwrap();
        let stream_id = Identifier::from_str(&self.stream)?;
        let topic_id = Identifier::from_str(&self.topic)?;

        // Create partitioning strategy
        let partitioning = match self.partitioning {
            PartitioningStrategy::PartitionId(id) => Partitioning::partition_id(id),
            PartitioningStrategy::Balanced => Partitioning::balanced(),
        };

        // Send messages to Iggy
        let mut messages_to_send = messages;
        match client.send_messages(&stream_id, &topic_id, &partitioning, &mut messages_to_send).await {
            Ok(_) => {
                info!("Sent {} messages to Iggy", messages_to_send.len());
                Ok(())
            }
            Err(e) => {
                error!("Failed to send messages to Iggy: {:?}", e);
                ctx.report_error("Could not write to Iggy", format!("{:?}", e)).await;

                // Back off and retry
                sleep(Duration::from_millis(1000)).await;
                Err(e.into())
            }
        }
    }
}

#[async_trait]
impl ArrowOperator for IggySinkFunc {
    fn name(&self) -> String {
        format!("iggy-sink-{}-{}", self.stream, self.topic)
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        // Initialize the client
        if let Err(e) = self.init_client().await {
            error!("Failed to initialize Iggy client: {:?}", e);
            ctx.report_error("Failed to initialize Iggy client", format!("{:?}", e)).await;
        }

        // Set timestamp column if available
        if let Some(schema) = ctx.out_schema.as_ref() {
            // Just log that we have a schema
            info!("Using schema: {:?}", schema);
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn arroyo_operator::context::Collector,
    ) {
        // Serialize the batch
        let values = self.serializer.serialize(&batch);

        // Get the timestamp column if available
        let timestamps = ctx.out_schema.as_ref()
            .and_then(|schema| Some(schema.timestamp_column(&batch)));

        // Create messages
        let mut messages = Vec::new();
        for (i, v) in values.into_iter().enumerate() {
            // Get timestamp if available (convert from nanos to millis)
            let _timestamp = timestamps.map(|ts| {
                if ts.is_null(i) {
                    None
                } else {
                    Some(ts.value(i) / 1_000_000)
                }
            }).flatten();

            // Create message
            let message = Message {
                id: 0, // Iggy will assign an ID
                length: v.len() as u32,
                payload: Bytes::from(v),
                headers: None, // No headers
            };

            messages.push(message);
        }

        // Send messages
        if let Err(e) = self.send_messages(messages, ctx).await {
            error!("Failed to send messages to Iggy: {:?}", e);
        }
    }

    async fn on_close(
        &mut self,
        _: &Option<SignalMessage>,
        _: &mut OperatorContext,
        _: &mut dyn arroyo_operator::context::Collector,
    ) {
        // Close the client connection
        if let Some(client) = &self.client {
            if let Err(e) = client.disconnect().await {
                warn!("Error disconnecting from Iggy: {:?}", e);
            }
        }
    }
}
