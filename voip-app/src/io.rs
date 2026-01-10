use std::sync::atomic::{AtomicU16, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use crate::jitter::JitterBuffer;
use crate::packet::AudioPacket;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, Host, Stream, StreamConfig, SampleRate, BufferSize};
use tokio::sync::broadcast::Sender;

pub struct AudioState {
    host: Host,
    input: Option<Stream>,
    output: Option<Stream>,
}

impl AudioState {
    pub fn new(host: Host) -> Self {
        AudioState {
            host,
            input: None,
            output: None,
        }
    }

    pub fn start(
        &mut self,
        channel: Sender<Vec<u8>>,
        output_jitter: Arc<Mutex<JitterBuffer>>,
    ) {
        let input_device = self.host.default_input_device().expect("No input device");
        let output_device = self.host.default_output_device().expect("No output device");

        let config = StreamConfig {
            channels: 1,
            sample_rate: 16000,
            buffer_size: BufferSize::Fixed(640),  // 40ms frames
        };

        println!("Using config: {:?}", config);

        self.output = output_stream_fn(output_device, output_jitter, config.clone()).ok().flatten();
        println!("Receiving audio...");

        self.input = input_stream_fn(input_device, channel, config).ok().flatten();
        println!("Sending audio...");
    }

    pub fn clear(&mut self) {
        self.input = None;
        self.output = None;
    }
}

pub fn input_stream_fn(
    input_device: Device,
    channel: Sender<Vec<u8>>,
    config: StreamConfig,
) -> Result<Option<Stream>, ()> {
    let seq = AtomicU16::new(0);

    let stream = input_device.build_input_stream(
        &config,
        move |data: &[f32], _: &cpal::InputCallbackInfo| {
            let samples: Vec<i16> = data.iter()
                .map(|&s| (s.clamp(-1.0, 1.0) * 32767.0) as i16)
                .collect();

            let packet = AudioPacket {
                seq: seq.fetch_add(1, Relaxed),
                samples,
            };

            
            let _ = channel.send(packet.serialize() );
            
        },
        |err| eprintln!("Input stream error: {:?}", err),
        None,
    );

    match stream {
        Ok(s) => {
            s.play().map_err(|_| ())?;
            Ok(Some(s))
        }
        Err(e) => {
            eprintln!("Failed to build input stream: {:?}", e);
            Err(())
        }
    }
}

pub fn output_stream_fn(
    output_device: Device,
    buffer: Arc<Mutex<JitterBuffer>>,
    config: StreamConfig,
) -> Result<Option<Stream>, ()> {
    let stream = output_device.build_output_stream(
        &config,
        move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
            let samples = {
                let mut jb = buffer.lock().unwrap();
                jb.pop_samples(data.len())
            };
            
            for (i, sample) in data.iter_mut().enumerate() {
                *sample = samples.get(i).copied().unwrap_or(0) as f32 / 32767.0;
            }
        },
        |err| eprintln!("Output stream error: {:?}", err),
        None,
    );

    match stream {
        Ok(s) => {
            s.play().map_err(|_| ())?;
            Ok(Some(s))
        }
        Err(e) => {
            eprintln!("Failed to build output stream: {:?}", e);
            Err(())
        }
    }
}
