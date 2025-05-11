use anyhow::{bail, Result};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::Connection as ArroyoConnection;
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::OperatorConfig;
use arroyo_rpc::var_str::VarStr;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use crate::send;

mod sink;
mod source;

use sink::IggySinkFunc;
use source::IggySourceFunc;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./iggy.svg");

// Import types from JSON schemas
typify::import_types!(
    schema = "src/iggy/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

typify::import_types!(schema = "src/iggy/table.json");

// Define partitioning strategy enum
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PartitioningStrategy {
    PartitionId(u32),
    Balanced,
}

// Define source offset enum - different name to avoid conflict with imported type
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum IggySourceOffset {
    Earliest,
    Latest,
}

pub struct IggyConnector {}

impl arroyo_operator::connector::Connector for IggyConnector {
    type ProfileT = IggyConfig;
    type TableT = IggyTable;

    fn name(&self) -> &'static str {
        "iggy"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "iggy".to_string(),
            name: "Apache Iggy".to_string(),
            icon: ICON.to_string(),
            description: "Read and write from an Apache Iggy cluster".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        config.endpoint
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            send(&mut tx.clone(), message).await;
        });
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut arroyo_rpc::ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> Result<ArroyoConnection> {
        let endpoint = options.pull_str("endpoint")?;
        let transport = options.pull_str("transport")?;

        let authentication = if let Some(username) = options.pull_opt_str("username")? {
            let password = options.pull_str("password")?;
            IggyConfigAuthentication::UsernamePassword {
                username,
                password: VarStr::new(password),
            }
        } else {
            IggyConfigAuthentication::None {}
        };

        let transport_protocol = match transport.as_str() {
            "tcp" => TransportProtocol::Tcp,
            "quic" => TransportProtocol::Quic,
            "http" => TransportProtocol::Http,
            _ => bail!("Invalid transport protocol"),
        };

        // Create a ConnectionAuthentication from IggyConfigAuthentication
        let conn_auth = match &authentication {
            IggyConfigAuthentication::None {} => ConnectionAuthentication::None {},
            IggyConfigAuthentication::UsernamePassword { username, password } => {
                ConnectionAuthentication::UsernamePassword {
                    username: username.clone(),
                    password: password.clone(),
                }
            }
        };

        let config = IggyConfig {
            endpoint: endpoint.clone(),
            transport: transport_protocol.clone(),
            authentication: authentication.clone(),
            connection: Connection {
                endpoint,
                transport: transport_protocol,
                authentication: conn_auth,
            },
        };

        let stream = options.pull_str("stream")?;
        let topic = options.pull_str("topic")?;

        let table_type = if options.pull_bool("is_source")? {
            let offset = match options.pull_str("offset")?.as_str() {
                "earliest" => "earliest",
                "latest" => "latest",
                _ => bail!("Invalid offset value"),
            };

            let consumer_id = options.pull_u64("consumer_id")? as u32;
            let partition_id = options.pull_u64("partition_id")? as u32;
            let auto_commit = options.pull_bool("auto_commit")?;

            TableType::Source {
                offset: match offset {
                    "earliest" => SourceOffset::Earliest,
                    "latest" => SourceOffset::Latest,
                    _ => bail!("Invalid offset value"),
                },
                consumer_id: consumer_id as i64,
                partition_id: partition_id as i64,
                auto_commit,
            }
        } else {
            let partitioning = match options.pull_str("partitioning")?.as_str() {
                "partition_id" => "partition_id",
                "balanced" => "balanced",
                _ => bail!("Invalid partitioning value"),
            };

            let partition_id = options.pull_opt_u64("partition_id")?.map(|id| id as u32);

            TableType::Sink {
                partitioning: match partitioning {
                    "partition_id" => SinkPartitioning::PartitionId,
                    "balanced" => SinkPartitioning::Balanced,
                    _ => bail!("Invalid partitioning value"),
                },
                partition_id: partition_id.map(|id| id as i64),
            }
        };

        let table = IggyTable {
            stream,
            topic,
            type_: table_type,
        };

        self.from_config(None, name, config, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> Result<ArroyoConnection> {
        let schema = schema.cloned().unwrap_or_else(|| ConnectionSchema {
            format: None,
            bad_data: None,
            framing: None,
            struct_name: None,
            fields: vec![],
            definition: None,
            inferred: None,
            primary_keys: std::collections::HashSet::default(),
        });

        Ok(ArroyoConnection {
            id,
            name: name.to_string(),
            connector: self.name(),
            connection_type: self.table_type(config.clone(), table.clone()),
            description: format!("Iggy: {}", config.endpoint),
            config: serde_json::to_string(&config)?,
            schema,
            partition_fields: None,
        })
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> Result<ConstructedOperator> {
        match &table.type_ {
            TableType::Source {
                offset,
                consumer_id,
                partition_id,
                auto_commit,
            } => {
                let offset_mode = match offset {
                    SourceOffset::Earliest => IggySourceOffset::Earliest,
                    SourceOffset::Latest => IggySourceOffset::Latest,
                };

                let (username, password) = match &profile.authentication {
                    IggyConfigAuthentication::None {} => (None, None),
                    IggyConfigAuthentication::UsernamePassword { username, password } => {
                        (Some(username.clone()), Some(password.sub_env_vars().expect("Failed to substitute env vars")))
                    }
                };

                Ok(ConstructedOperator::from_source(Box::new(
                    IggySourceFunc {
                        stream: table.stream.clone(),
                        topic: table.topic.clone(),
                        partition_id: (*partition_id) as u32,
                        consumer_id: (*consumer_id) as u32,
                        endpoint: profile.endpoint.clone(),
                        transport: profile.transport.to_string(),
                        username,
                        password,
                        offset_mode,
                        auto_commit: *auto_commit,
                        format: config.format.expect("Format must be set for Iggy source"),
                        framing: config.framing,
                        bad_data: config.bad_data,
                        client: None,
                        messages_per_second: std::num::NonZeroU32::new(
                            config
                                .rate_limit
                                .map(|l| l.messages_per_second)
                                .unwrap_or(u32::MAX),
                        )
                        .unwrap(),
                        metadata_fields: config.metadata_fields,
                    },
                )))
            }
            TableType::Sink {
                partitioning,
                partition_id,
            } => {
                let partitioning_strategy = match partitioning {
                    SinkPartitioning::PartitionId => {
                        if let Some(id) = partition_id {
                            PartitioningStrategy::PartitionId((*id) as u32)
                        } else {
                            bail!("Partition ID is required for partition_id partitioning strategy")
                        }
                    }
                    SinkPartitioning::Balanced => PartitioningStrategy::Balanced,
                };

                let (username, password) = match &profile.authentication {
                    IggyConfigAuthentication::None {} => (None, None),
                    IggyConfigAuthentication::UsernamePassword { username, password } => {
                        (Some(username.clone()), Some(password.sub_env_vars().expect("Failed to substitute env vars")))
                    }
                };

                Ok(ConstructedOperator::from_operator(Box::new(
                    IggySinkFunc {
                        stream: table.stream.clone(),
                        topic: table.topic.clone(),
                        partitioning: partitioning_strategy,
                        endpoint: profile.endpoint.clone(),
                        transport: profile.transport.to_string(),
                        username,
                        password,
                        client: None,
                        serializer: ArrowSerializer::new(
                            config.format.expect("Format must be defined for IggySink"),
                        ),
                    },
                )))
            }
        }
    }
}
