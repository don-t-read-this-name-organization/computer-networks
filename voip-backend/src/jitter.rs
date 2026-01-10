use std::collections::VecDeque;

pub struct JitterBuffer {
    buffer: VecDeque<i16>,
}

impl JitterBuffer {
    pub fn new() -> Self {
        Self {
            buffer: VecDeque::new(),
        }
    }

    pub fn push_packet(&mut self, samples: &[i16]) {
        for &s in samples {
            self.buffer.push_back(s);
        }
        // Limit buffer to reduce delay - 20ms * 2 = 40ms max delay
        while self.buffer.len() > 1920 { // 40ms at 48kHz
            self.buffer.pop_front();
        }
    }

    pub fn pop_sample(&mut self) -> i16 {
        self.buffer.pop_front().unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}
