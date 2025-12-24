#[derive(Debug, Clone)]
pub struct AudioPacket {
    pub seq: u16,
    pub samples: Vec<i16>,
}

impl AudioPacket {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.samples.len() * 2);
        buf.extend_from_slice(&self.seq.to_le_bytes());
        for s in &self.samples {
            buf.extend_from_slice(&s.to_le_bytes());
        }
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 2 {
            return None;
        }

        let seq = u16::from_le_bytes([data[0], data[1]]);
        let mut samples = Vec::new();

        for chunk in data[2..].chunks_exact(2) {
            samples.push(i16::from_le_bytes([chunk[0], chunk[1]]));
        }

        Some(Self { seq, samples })
    }
}
