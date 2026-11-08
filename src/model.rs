use serde::{Deserialize, Serialize};

pub const GROUP_MEMBER_CAP: i64 = 1_000;
pub const CHANNEL_MEMBER_CAP: i64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Group,
    Channel,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Group => "group",
            Kind::Channel => "channel",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "group" => Some(Kind::Group),
            "channel" => Some(Kind::Channel),
            _ => None,
        }
    }

    pub fn member_cap(self) -> i64 {
        match self {
            Kind::Group => GROUP_MEMBER_CAP,
            Kind::Channel => CHANNEL_MEMBER_CAP,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Author {
    pub id: i64,
    pub login: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub chat_id: i64,
    pub seq: i64,
    pub author: Author,
    pub body: String,
    pub ts: i64,
}

/// Builds the `{"t":"msg", ...message}` WebSocket frame.
pub fn msg_frame(msg: &Message) -> String {
    let json = serde_json::to_string(msg).expect("message serializes");
    format!(r#"{{"t":"msg",{}"#, &json[1..])
}
