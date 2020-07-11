

export interface IConsumer {
    connect(): Promise<this>;

    onError(err: Error): void;

    onMessage(handler: IMessageHandler): any | void;
}

export interface IConsumerConfig {
    GROUP_ID: string;
    BOOTSTRAP_SERVERS: string;

    toRDKafka(): object;
}

export interface IConsumerMessage {
    value: Buffer
    size: number, // size of the message, in bytes
    topic: string, // topic the message comes from
    offset: number, // offset the message was read from
    partition: number, // partition the message was on
    key: any
}

export class ConsumerMessage implements IConsumerMessage {
    private _size: number;
    private _topic: string;
    private _offset: number;
    private _partition: number;
    private _key: any;
    private _value: Buffer;

    constructor({topic, key, value}: { topic: string, key: any, value: Buffer }) {
        this._topic = topic;
        this._key = key;
        this._value = value;
    }

    get size(): number {
        return this._size;
    }

    set size(value: number) {
        this._size = value;
    }

    get topic(): string {
        return this._topic;
    }

    set topic(value: string) {
        this._topic = value;
    }

    get offset(): number {
        return this._offset;
    }

    set offset(value: number) {
        this._offset = value;
    }

    get partition(): number {
        return this._partition;
    }

    set partition(value: number) {
        this._partition = value;
    }

    get key(): any {
        return this._key;
    }

    set key(value: any) {
        this._key = value;
    }

    get value(): Buffer {
        return this._value;
    }

    set value(value: Buffer) {
        this._value = value;
    }
}

export interface IMessageHandler {
    (IConsumerMessage): any | void
}

export class ConsumerConfig implements IConsumerConfig {
    GROUP_ID: string;
    BOOTSTRAP_SERVERS: string

    constructor(bootstrapServers:string[] | string, groupId: string)
    constructor(host: string, port: string, groupId: string)
    constructor(hostOrBootstrapServers: string, portOrGroupId: string, groupId?: string) {
        if (groupId) {
            this.GROUP_ID = groupId;
            this.BOOTSTRAP_SERVERS = `${hostOrBootstrapServers}:${portOrGroupId}`;
        } else {
            this.GROUP_ID = portOrGroupId;
            this.BOOTSTRAP_SERVERS = Array.isArray(hostOrBootstrapServers) ? hostOrBootstrapServers.join(',') : hostOrBootstrapServers;
        }
    }
    toRDKafka(): object {
        return {
            "bootstrap.servers": this.BOOTSTRAP_SERVERS,
            "group.id": this.GROUP_ID
        };
    }
}

export interface IConsumerConstructor {//rdkafka.Consumer presumably
    new(config: object): any
}

export class SimpleConsumer implements IConsumer {
    handlers: Array<IMessageHandler>;
    connecting: boolean;
    connected: boolean;
    consumer: any;
    topics: Array<string>;

    create(Consumer: IConsumerConstructor, topics: Array<string>, config: ConsumerConfig) {
        const rdkafkaConfig = config.toRDKafka();
        this.consumer = new Consumer(rdkafkaConfig);
        this.topics = topics;
        this.handlers = [];
        return this;
    }

    connect(): Promise<this> {
        return new Promise((resolve, reject) => {
            if (this.connected) {
                reject(new Error("Already connected"))
            } else {
                this.consumer.on("ready", () => {
                    this.connected = true;
                    this.connecting = false;
                    this.subscribe(this.topics)
                    resolve(this);
                })
                this.consumer.on("event.error", err => {
                    this.onError(err);
                })

                this.consumer.connect({}, (err, res) => {
                    if (err) return reject(err)
                })
                this.connecting = true;
            }
        })
    }

    private subscribe(topics: Array<string>) {
        this.consumer.subscribe(topics)
        this.consumer.consume()
    }

    onMessage(handler: IMessageHandler) {
        this.handlers.push(handler)
        this.consumer.on("data", msg => handler(new ConsumerMessage(msg)))
        return this;
    }

    onError(err: Error): void {
        console.error("[SimpleConsumer] - ", err)
    }

    onDisconnect(cb: Function): void {
        this.consumer.on("disconnected", cb)
    }
}


