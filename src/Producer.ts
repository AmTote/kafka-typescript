export interface IProducer {
  connect(): Promise<this>;

  onError(err: Error): void;

  // RDKafka demo shows string as key -- it may not serialize other values
  send(key: any, value: Buffer, topic?: string): IProducer
}

export interface IProducerConfig {
  BOOTSTRAP_SERVERS: string;

  toRDKafka(): object
}

export class ProducerConfig implements IProducerConfig {
  public BOOTSTRAP_SERVERS: string;

  constructor(host: string, port: string)
  constructor(bootstrapServers: string | string[])
  constructor(hostOrBootstrapServers: string | string[], port?: string) {
    if (port) {
        this.BOOTSTRAP_SERVERS = `${hostOrBootstrapServers}:${port}`;
    } else if (Array.isArray(hostOrBootstrapServers)) {
        this.BOOTSTRAP_SERVERS = hostOrBootstrapServers.join(',');
    } else {
        this.BOOTSTRAP_SERVERS = hostOrBootstrapServers;
    }
  }

  toRDKafka(): object {
    return {
        "bootstrap.servers": this.BOOTSTRAP_SERVERS
    };
  }
}


export interface IProducerConstructor {//rdkafka.Producer presumably
  new(config: object): any
}

export class SimpleProducer implements IProducer {
  getTopic(): string {
    return this.topic;
  }

  setTopic(value: string) {
    this.topic = value;
  }

  connecting: boolean;
  connected: boolean;
  producer: any;

  topic: string;
  config: ProducerConfig;

  create(Producer: IProducerConstructor, config: ProducerConfig) {
    let rdkafkaConfig = config.toRDKafka();
    this.producer = new Producer(rdkafkaConfig);
    this.config = config
    return this;
  }

  connect(): Promise<this> {
    return new Promise((resolve, reject) => {
      if (this.connected) {
        resolve(this)
      } else {
        this.producer.connect({}, (err, res) => {
          if (err) return reject(err)
        })
        this.connecting = true;
        this.producer.on("ready", () => {
          this.connected = true;
          this.connecting = false;
          resolve(this);
        })
        this.producer.on("event.error", err => {
          if (!this.connected) { // When promise not fulfilled yet
            return reject(err);
          }
          this.onError(err);
        })

      }
    })
  }

  send(key: any, value: Buffer, topic?: string) {
    let t = this.topic || topic
    if (!this.connected) {
      this.connect()
          .then(_ => this.producer.produce(t, null, value, key))
    }
    this.producer.produce(t, null, value, key)
    return this;
  }

  onError(err: Error): void {
    console.error("[SimpleProducer] - ", err)
  }
}

