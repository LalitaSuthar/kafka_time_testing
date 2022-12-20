use rdkafka::{
    consumer::{ BaseConsumer, Consumer, ConsumerContext, Rebalance },
    message::ToBytes,
    producer::{ BaseProducer, BaseRecord, Producer, ProducerContext, ThreadedProducer },
    ClientConfig,
    ClientContext,
    Message,
    Offset,
};
use std::{ thread, time::Duration, time::Instant };

pub fn consumer_process() {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "my_consumer_group")
        .create()
        .expect("invalid consumer config");

    consumer.subscribe(&["rust"]).expect("topic subscribe failed");

    // println!("{:?}",consumer );

    let mss = consumer.poll(Duration::from_millis(1000));
    println!("1");
    for ms in mss.iter() {
        println!("2");

        let msg = ms.as_ref().unwrap();
        let key: &str = msg.key_view().unwrap().unwrap();
        let value = msg.payload().unwrap();

        let user: User = serde_json::from_slice(value).expect("failed to deserialize JSON to User");
        println!(
            "received key {} with value {:?} in offset {:?} from partition {}",
            key,
            user,
            msg.offset(),
            msg.partition()
        );
        //     consumer.unsubscribe();


        // for m in ms.messages() {
        //     println!(
        //         "{}:{}@{}: {:?}",
        //         ms.topic(),
        //         ms.partition(),
        //         m.offset,
        //         m.value
        //     );
        // }
    }

    // thread::spawn(move || loop {
    // for msg_result in consumer.iter() {
    //     let msg = msg_result.unwrap();
        //     let key: &str = msg.key_view().unwrap().unwrap();
        //     let value = msg.payload().unwrap();

        //     let user: User =
        //         serde_json::from_slice(value).expect("failed to deserialize JSON to User");
        //     println!(
        //         "received key {} with value {:?} in offset {:?} from partition {}",
        //         key,
        //         user,
        //         msg.offset(),
        //         msg.partition()
        //     );
            // consumer.unsubscribe();
        // println!("msg::{:?}", msg);
        // println!("{:?}","connection close" );
    // }
    // });
}

use serde::{ Deserialize, Serialize };
#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: i32,
}

pub struct ConsumerCallbackLogger;
impl ClientContext for ConsumerCallbackLogger {}
impl ConsumerContext for ConsumerCallbackLogger {
    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        offsets: &rdkafka::TopicPartitionList
    ) {
        match result {
            Ok(_) => {
                for e in offsets.elements() {
                    match e.offset() {
                        //skip Invalid offset
                        Offset::Invalid => {}
                        _ => {
                            println!(
                                "committed offset {:?} in partition {}",
                                e.offset(),
                                e.partition()
                            );
                        }
                    }
                }
            }
            Err(err) => { println!("error committing offset - {}", err) }
        }
    }
}