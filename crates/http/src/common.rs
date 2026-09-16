use std::sync::Arc;

use aquatic_common::access_list::AccessListArcSwap;
use aquatic_common::CanonicalSocketAddr;

pub use aquatic_common::ValidUntil;

use aquatic_http_protocol::{
    common::InfoHash,
    request::{AnnounceRequest, ScrapeRequest},
    response::{AnnounceResponse, ScrapeResponse},
};
use glommio::channels::shared_channel::SharedSender;
use slotmap::new_key_type;

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub struct ConsumerId(pub usize);

new_key_type! {
    pub struct ConnectionId;
}

/// Basic information about a known torrent, used for the index view
#[derive(Debug, Clone, Copy)]
pub struct TorrentSummary {
    pub info_hash: InfoHash,
    pub seeders: usize,
    pub leechers: usize,
}

/// A swarm worker's response to an index request
#[derive(Debug)]
pub struct IndexResponse {
    pub torrents: Vec<TorrentSummary>,
}

#[derive(Debug)]
pub enum ChannelRequest {
    Announce {
        request: AnnounceRequest,
        peer_addr: CanonicalSocketAddr,
        response_sender: SharedSender<AnnounceResponse>,
    },
    Scrape {
        request: ScrapeRequest,
        peer_addr: CanonicalSocketAddr,
        response_sender: SharedSender<ScrapeResponse>,
    },
    Index {
        response_sender: SharedSender<IndexResponse>,
    },
}

#[derive(Default, Clone)]
pub struct State {
    pub access_list: Arc<AccessListArcSwap>,
}
