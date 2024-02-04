
use log::LevelFilter;
use log::{info, warn};

use env_logger;


fn setup_logging() {
    let _ = env_logger::builder()
        .filter_level(LevelFilter::Trace)
        .is_test(true)
        .try_init();
}

#[test]
fn test() {
    setup_logging();

    info!("This is an informational message during testing.");
    warn!("This is a warning message during testing.");
    // error!("This is an error message during testing.");
}



