const thrift = require('thrift');
const sasl = require('saslmechanisms');

const saslFactory = new sasl.Factory();
saslFactory.use(require('sasl-plain'));

const START = 1;
const OK = 2;
const BAD = 3;
const ERROR = 4;
const COMPLETE = 5;

class Frame {
  constructor() {
    this.buffer = Buffer.alloc(0);
  }

  read(data, offset) {
    offset = offset || 0;
    if (!this.length) {
      this.length = data.readUInt32BE(offset);
      this.buffer = Buffer.alloc(this.length);
      this.writeCursor = 0;
      offset += 4;
    }
    const pendingRead = this.length - this.writeCursor;
    const dataEnd = offset + pendingRead;
    const requireMoreData = dataEnd > data.length;
    this.fullyRead = !requireMoreData;
    const sourceEnd = requireMoreData ? data.length : dataEnd;
    data.copy(this.buffer, this.writeCursor, offset, sourceEnd);
    this.writeCursor = this.writeCursor + (sourceEnd - offset);

    let frames = [this];

    if (this.fullyRead && dataEnd < data.length) {
      const nextFrame = new Frame();
      frames = frames.concat(nextFrame.read(data, dataEnd));
    }
    return frames;
  }
}

module.exports = (authzid, username, password) => {
  let saslComplete = false;
  let saslSent = false;
  let pendingData = [];
  let lastWriteCallback;

  const sendFrame = (data, seqId, writeCallback) => {
    const frameBuffer = Buffer.alloc(4 + data.length);
    frameBuffer.writeUInt32BE(data.length, 0);
    data.copy(frameBuffer, 4, 0, data.length);
    writeCallback(frameBuffer, seqId);
  };

  const flushPendingData = () => {
    pendingData.forEach(([data, seqId]) => {
      sendFrame(data, seqId, lastWriteCallback);
    });
    pendingData = [];
  };

  class TSaslTransport extends thrift.TBufferedTransport {
    constructor(buffer, callback) {
      super(buffer, (data, seqId) => {
        if (!saslComplete) {
          pendingData.push([data, seqId]);
          lastWriteCallback = callback;
          if (!saslSent) {
            const selectedMechanism = 'PLAIN';
            const mechanism = saslFactory.create([selectedMechanism]);
            const payload = mechanism.response({ authzid, username, password: password || 'None' });
            saslSent = true;
            TSaslTransport.sendSaslMessage(START, Buffer.from(selectedMechanism, 'utf-8'), callback);
            TSaslTransport.sendSaslMessage(OK, Buffer.from(payload, 'utf-8'), callback);
          }
        } else {
          sendFrame(data, seqId, callback);
        }
      });
    }

    static receiver(callback, seqid) {
      const receiver = thrift.TBufferedTransport.receiver(callback, seqid);

      let frame = null;

      return (data) => {
        if (!saslComplete) {
          thrift.TBufferedTransport.receiver((transport) => {
            const { status, payload } = TSaslTransport.receiveSaslMessage(transport);
            if (status !== COMPLETE) {
              throw new Error(`SASL Failed with status ${status}: ${payload.toString('utf-8')}`);
            }
            saslComplete = true;
            flushPendingData();
          })(data);
        } else {
          if (!frame) {
            frame = new Frame();
          }
          const frames = frame.read(data, 0);

          frames.filter(f => f.fullyRead).map(f => receiver(f.buffer));
          frame = frames.find(f => !f.fullyRead);
        }
      };
    }

    static sendSaslMessage(status, payload, callback) {
      const saslTransport = new thrift.TBufferedTransport(null, callback);
      const messageHeader = Buffer.alloc(5);
      messageHeader.writeInt8(status);
      messageHeader.writeUInt32BE(payload.length, 1);
      saslTransport.write(messageHeader);
      saslTransport.write(payload);
      saslTransport.flush();
    }

    static receiveSaslMessage(transport) {
      const buffer = transport.read(5);
      const status = buffer.readInt8();
      const payloadSize = buffer.readUInt32BE(1);
      if (payloadSize < 0 || payloadSize > 104857600) {
        throw new Error(`Incorrect payload size in SASL message: ${payloadSize}`);
      }
      const payload = transport.read(payloadSize);
      if (status === BAD || status === ERROR) {
        throw new Error(`SASL Error: ${payload.toString('utf-8')}`);
      }
      return { status, payload };
    }
  }

  return TSaslTransport;
};
