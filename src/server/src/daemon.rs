use crate::server_state::ServerState;
use std::thread;
use std::time::Duration;

// <strip milestone="silent">
//use common::PAGE_SLOTS;

// const dirty_percent: f64 = 0.5;
// </strip>

#[allow(dead_code)]
pub(crate) struct Daemon {
    _server_state: &'static ServerState,
    pub(crate) _thread: Option<thread::JoinHandle<()>>,
}

#[allow(dead_code)]
impl Daemon {
    pub(crate) fn new(server_state: &'static ServerState, sleep_sec: u64) -> Self {
        // This should be async or moved into the workers
        let thread = std::thread::spawn(move || loop {
            debug!("Daemon doing stuff");
            // <strip milestone="silent">

            // Flush the dirty pages of storage manager of server state if percentage
            // of dirty pages is greater than or equal to dirty_percent
            // TODO(merge): This is a temporary solution. We should have a wrapper in SM that optionally calls. BP should not be exposed
            // if server_state.managers.sm.get_dirty_page_count() as f64 / PAGE_SLOTS as f64 >= dirty_percent {
            //     server_state.managers.sm.flush();
            // }
            // </strip>

            thread::sleep(Duration::new(sleep_sec, 0));
        });

        Daemon {
            _server_state: server_state,
            _thread: Some(thread),
        }
    }
}
