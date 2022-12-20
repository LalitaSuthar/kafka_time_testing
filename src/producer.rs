use rdkafka::{
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance},
    message::ToBytes,
    producer::{BaseProducer, BaseRecord, Producer, ProducerContext, ThreadedProducer},
    ClientConfig, ClientContext, Message, Offset,
};
use std::{thread, time::Duration , time::Instant};


pub fn producer_process(){
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("invalid producer config");

    // for i in 0..1 {
        println!("sending message");

        let user = User {
            id: 1,
        };

        let user_json = serde_json::to_string_pretty(&user).expect("json serialization failed");

        producer
            .send(
                BaseRecord::to("rust")
                    .key(&format!("user-{}", 1))
                    .payload(&user_json),
            )
            .expect("failed to send message");

        // thread::sleep(Duration::from_millis(100));
        // let start = Instant::now();
        // println!("{:?}",start );

    // }

}

use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: i32,
}

pub struct ProduceCallbackLogger;

impl ClientContext for ProduceCallbackLogger {}

impl ProducerContext for ProduceCallbackLogger {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &rdkafka::producer::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        let dr = delivery_result.as_ref();
        //let msg = dr.unwrap();

        match dr {
            Ok(msg) => {
                let key: &str = msg.key_view().unwrap().unwrap();
                println!(
                    "produced message with key {} in offset {} ",
                    key,
                    msg.offset()
                )
            }
            Err(producer_err) => {
                let key: &str = producer_err.1.key_view().unwrap().unwrap();

                println!(
                    "failed to produce message with key {} - {}",
                    key, producer_err.0,
                )
            }
        }
    }
}