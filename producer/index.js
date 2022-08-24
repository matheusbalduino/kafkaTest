console.log("producer...")
import Kafka from 'node-rdkafka';
import eventType from '../eventType.js'

const stream = Kafka.Producer.createWriteStream({'metadata.broker.list':'localhost:9092'}, {}, { topic: 'test' });

function getRandomAnimal(){
  const categories = ['CAT', 'DOG']
  return categories[Math.floor(Math.random() * categories.length)]
}

function getRandomNoise(category){
  if(category === 'CAT'){
    const categories = ['meow', 'puur']
    return categories[Math.floor(Math.random() * categories.length)]
  }
  else{
    const categories = ['woof', 'bark']
    return categories[Math.floor(Math.random() * categories.length)]
  }
}

function queueMessage(){
  const category = getRandomAnimal();
  const noise = getRandomNoise(category);
  const event = { category: category, noise: noise }
  const success = stream.write(eventType.toBuffer(event));
  if(success) console.log('Message wrote successfully to stream');
  else console.log('something went wrong');
}

setInterval(() => {
  queueMessage();
}, 3000 )