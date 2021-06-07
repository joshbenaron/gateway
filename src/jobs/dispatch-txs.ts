import { getQueueUrl, createQueueHandler } from "../lib/queues";
import { publish } from "../lib/pub-sub";
import { get } from "../lib/buckets";
import { broadcastTx } from "../lib/broadcast";
import { ImportTx, DispatchTx } from "../interfaces/messages";
import { toB64url } from "../lib/encoding";
import { Transaction } from "../lib/arweave";

export const handler = createQueueHandler<DispatchTx>(
  getQueueUrl("dispatch-txs"),
  async (message) => {
    console.log(message);
    const { tx, data_size: dataSize, data_format } = message;

    console.log(`data_size: ${dataSize}, tx: ${tx.id}`);

    console.log(`broadcasting: ${tx.id}`);

    const fullTx: Transaction = {
      ...tx,
      data:
        (!data_format || data_format < 2.1) && dataSize > 0
          ? await getEncodedData(tx.id)
          : "",
    };

    await broadcastTx(fullTx, [
      "http://sfo-1.na-west-1.arweave.net:1984",
      "http://fra-2.eu-central-2.arweave.net:1984",
      "http://nyc-2.na-east-1.arweave.net:1984",
      "http://gateway-1.arweave.net:1984",
      "http://gateway-2.arweave.net:1984",
      "http://gateway-3.arweave.net:1984",
      "http://gateway-4.arweave.net:1984",
      "http://gateway-5.arweave.net:1984",
      "http://gateway-6.arweave.net:1984",
    ]);

    console.log(`publishing: ${tx.id}`);

    await publish<ImportTx>(message);
  }
);

const getEncodedData = async (txid: string): Promise<string> => {
  try {
    const data = await get("tx-data", `tx/${txid}`);
    return toB64url(data.Body as Buffer);
  } catch (error) {
    return "";
  }
};
