pub struct Stack {
    data: Vec<u8>
}

impl Stack {
    pub fn new() -> Stack {
        Stack {
            data: Vec::new()
        }
    }

    pub fn push(&mut self, val: u8) {
        self.data.push(val);
    } 

    pub fn pop(&mut self) -> Option<u8> {
        self.data.pop()
    }
}