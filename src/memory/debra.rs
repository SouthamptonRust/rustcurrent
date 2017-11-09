use std::sync::atomic::{AtomicUsize, Ordering};

struct DEBRAReclaimer {
    
}

struct GlobalEpoch {
    epoch: AtomicUsize,
    // threads: list of all other threads - research
    // garbage: global garbage bag
}

impl DEBRAReclaimer {
    pub fn new() -> DEBRAReclaimer {
        DEBRAReclaimer  {
            
        }
    }

    pub fn enter_managed() {
        unimplemented!()
    }

    pub fn exit_managed() {
        unimplemented!()
    }

    pub fn retire() {
        // Need to add the argument here, presumably an Arc
        unimplemented!()
    }
}

impl GlobalEpoch {
    pub fn new() -> GlobalEpoch {
        GlobalEpoch {
            epoch: AtomicUsize::new(0)
        }
    }

    pub fn attempt_increment(&mut self) -> bool {
        let current = self.epoch.load(Ordering::Relaxed);
        self.epoch.compare_and_swap() == current
    }
}