use std::collections::VecDeque;

pub struct JitterBuffer {
    buffer: VecDeque<i16>,
    min_buffer: usize,
    max_buffer: usize,
    ready: bool,
}

impl JitterBuffer {
    pub fn new() -> Self {
        Self {
            buffer: VecDeque::with_capacity(32000),
            min_buffer: 4800,     // 300ms at 16kHz
            max_buffer: 12800,    // 800ms max delay
            ready: false,
        }
    }

    pub fn push_packet(&mut self, samples: &[i16]) {
        self.buffer.extend(samples);
        
        if !self.ready && self.buffer.len() >= self.min_buffer {
            self.ready = true;
            println!("[JITTER] Buffer ready, len: {}", self.buffer.len());
        }
        
        // Drop oldest if too full
        while self.buffer.len() > self.max_buffer {
            self.buffer.pop_front();
        }
    }

    /// Pop multiple samples at once - more efficient than per-sample
    pub fn pop_samples(&mut self, count: usize) -> Vec<i16> {
        if !self.ready {
            return vec![0; count];
        }
        
        // Keep playing even if buffer is low - only stop if completely empty
        if self.buffer.is_empty() {
            self.ready = false;
            println!("[JITTER] Buffer empty, waiting for refill");
            return vec![0; count];
        }
        
        // If we have some samples but not enough, pad with zeros
        let available = self.buffer.len().min(count);
        let mut result = Vec::with_capacity(count);
        
        for _ in 0..available {
            result.push(self.buffer.pop_front().unwrap_or(0));
        }
        
        // Pad remaining with zeros if needed
        while result.len() < count {
            result.push(0);
        }
        
        result
    }

    pub fn pop_sample(&mut self) -> i16 {
        if !self.ready || self.buffer.is_empty() {
            return 0;
        }
        self.buffer.pop_front().unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.ready = false;
    }
}
