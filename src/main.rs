// fn main() {
//     println!("Hello, world!");
// }
use std::{thread, time::Duration , time::Instant};
mod consumer;
use consumer::consumer_process;
mod producer;
use producer::producer_process;

fn main() {

    let start = Instant::now();
    producer_process();
    consumer_process();
    let duration = start.elapsed();

    println!("Time elapsed in expensive_function() is: {:?}", duration);



    

    // let producer: ThreadedProducer<ProduceCallbackLogger> = ClientConfig::new()
    //     .set("bootstrap.servers", "localhost:9092")
    //     .create_with_context(ProduceCallbackLogger {})
    //     .expect("invalid producer config");

    // let start = Instant::now();
    // println!("\n\nstart:::  {:?}\n",start );

    // for i in 0..1 {
    //     println!("sending message");

    //     let user = User {
    //         id: i,
    //     };

    //     let user_json = serde_json::to_string_pretty(&user).expect("json serialization failed");

    //     producer
    //         .send(
    //             BaseRecord::to("rust")
    //                 .key(&format!("user-{}", i))
    //                 .payload(&user_json),
    //         )
    //         .expect("failed to send message");

    //     thread::sleep(Duration::from_millis(100));
    //     // let start = Instant::now();
    //     // println!("{:?}",start );

    // }

    // let consumer: BaseConsumer<ConsumerCallbackLogger> = ClientConfig::new()
    //     .set("bootstrap.servers", "localhost:9092")
    //     .set("group.id", "my_consumer_group")
    //     .create_with_context(ConsumerCallbackLogger {})
    //     .expect("invalid consumer config");

    // consumer
    //     .subscribe(&["rust"])
    //     .expect("topic subscribe failed");

    // thread::spawn(move || loop {
    //     for msg_result in consumer.iter() {
    //         let msg = msg_result.unwrap();
    //         let key: &str = msg.key_view().unwrap().unwrap();
    //         let value = msg.payload().unwrap();
    //         let user: User =
    //             serde_json::from_slice(value).expect("failed to deserialize JSON to User");
    //         println!(
    //             "received key {} with value {:?} in offset {:?} from partition {}",
    //             key,
    //             user,
    //             msg.offset(),
    //             msg.partition()
    //         );
    //         let duration = start.elapsed();
    //         println!("duration ::: {:?}",duration );

    //     }
    // });
}

// use serde::{Deserialize, Serialize};
// #[derive(Serialize, Deserialize, Debug)]
// struct User {
//     id: i32,
// }

// pub struct ConsumerCallbackLogger;
// impl ClientContext for ConsumerCallbackLogger {}
// impl ConsumerContext for ConsumerCallbackLogger {
//     fn commit_callback(
//         &self,
//         result: rdkafka::error::KafkaResult<()>,
//         offsets: &rdkafka::TopicPartitionList,
//     ) {
//         match result {
//             Ok(_) => {
//                 for e in offsets.elements() {
//                     match e.offset() {
//                         //skip Invalid offset
//                         Offset::Invalid => {}
//                         _ => {
//                             println!(
//                                 "committed offset {:?} in partition {}",
//                                 e.offset(),
//                                 e.partition()
//                             )
//                         }
//                     }
//                 }
//             }
//             Err(err) => {
//                 println!("error committing offset - {}", err)
//             }
//         }
//     }
// }

// pub struct ProduceCallbackLogger;

// impl ClientContext for ProduceCallbackLogger {}

// impl ProducerContext for ProduceCallbackLogger {
//     type DeliveryOpaque = ();

//     fn delivery(
//         &self,
//         delivery_result: &rdkafka::producer::DeliveryResult<'_>,
//         _delivery_opaque: Self::DeliveryOpaque,
//     ) {
//         let dr = delivery_result.as_ref();
//         //let msg = dr.unwrap();

//         match dr {
//             Ok(msg) => {
//                 let key: &str = msg.key_view().unwrap().unwrap();
//                 println!(
//                     "produced message with key {} in offset {} ",
//                     key,
//                     msg.offset()
//                 )
//             }
//             Err(producer_err) => {
//                 let key: &str = producer_err.1.key_view().unwrap().unwrap();

//                 println!(
//                     "failed to produce message with key {} - {}",
//                     key, producer_err.0,
//                 )
//             }
//         }
//     }
// }