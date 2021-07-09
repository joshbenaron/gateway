import ProgressBar from 'progress';
import {DataItemJson} from 'arweave-bundles';
import {config} from 'dotenv';
import {getLastBlock} from '../utility/height.utility';
import {serializeBlock, serializeTransaction, serializeAnsTransaction, serializeTags} from '../utility/serialize.utility';
import {streams, initStreams, resetCacheStreams} from '../utility/csv.utility';
import {log} from '../utility/log.utility';
import {ansBundles} from '../utility/ans.utility';
import {mkdir} from '../utility/file.utility';
import {sleep} from '../utility/sleep.utility';
import {TestSuite} from '../utility/mocha.utility';
import {getNodeInfo} from '../query/node.query';
import {block} from '../query/block.query';
import {transaction, tagValue, Tag} from '../query/transaction.query';
import {getDataFromChunks} from '../query/node.query';
import {importBlocks, importTransactions, importTags} from './import.database';
import {DatabaseTag} from './transaction.database';
import {cacheAns102Entries} from '../caching/ans.entry.caching';

config();
mkdir('snapshot');
mkdir('cache');

export const storeSnapshot = process.env.SNAPSHOT === '1' ? true : false;
export const parallelization = parseInt(process.env.PARALLEL || '8');

export let SIGINT: boolean = false;
export let SIGKILL: boolean = false;
export let bar: ProgressBar;
export let topHeight = 0;
export let currentHeight = 0;
export let timer = setTimeout(() => {}, 0);

export function configureSyncBar(start: number, end: number) {
  bar = new ProgressBar(
      ':current/:total blocks synced [:bar] :percent :etas',
      {
        complete: '|',
        incomplete: ' ',
        total: end - start,
      },
  );
}

/**
 * Entrypoint for the cron job
 */
export async function startSync() {
  const startHeight = await getLastBlock();
  currentHeight = startHeight;

  if (parallelization > 0) {
    log.info(`[database] starting sync, parallelization is set to ${parallelization}`);
    if (storeSnapshot) {
      log.info('[snapshot] also writing new blocks to the snapshot folder');
    }

    initStreams();
    signalHook();

    if (startHeight > 0) {
      const nodeInfo = await getNodeInfo();
      configureSyncBar(startHeight, nodeInfo.height);
      topHeight = nodeInfo.height;

      log.info(`[database] database is currently at height ${startHeight}, resuming sync to ${topHeight}`);

      bar.tick();
      await parallelize(startHeight + 1);
    } else {
      const nodeInfo = await getNodeInfo();
      configureSyncBar(0, nodeInfo.height);
      topHeight = nodeInfo.height;
      log.info(`[database] syncing from block 0 to ${topHeight}`);
      bar.tick();
      await parallelize(0);
    }
  }
}

/**
 * A cron job get all transactions from the given block height onwards
 * Once the last block is stored and indexed, the cron job will check every 30 seconds
 *
 * @param {number} height - block height to start from
 */
export async function parallelize(height: number) {
  clearTimeout(timer);
  timer = setTimeout(async () => {
    log.info('[database] sync timed out, restarting server');
    process.exit();
  }, 300 * 1000);

  currentHeight = height;

  if (height >= topHeight) {
    log.info('[database] fully synced, monitoring for new blocks');
    await sleep(30000);
    const nodeInfo = await getNodeInfo();
    if (nodeInfo.height > topHeight) {
      log.info(`[database] updated height from ${topHeight} to ${nodeInfo.height} syncing new blocks`);
      topHeight = nodeInfo.height;
    }

    await parallelize(height);
  } else {
    const batch = [];

    for (let i = height; i < height + parallelization && i < topHeight; i++) {
      batch.push(storeBlock(i));
    }

    SIGINT = true;

    await Promise.all(batch);

    try {
      await importBlocks(`${process.cwd()}/cache/block.csv`);
    } catch (error) {
      log.error('[sync] importing new blocks failed most likely due to it already being in the DB');
      log.error(error);
    }

    try {
      await importTransactions(`${process.cwd()}/cache/transaction.csv`);
    } catch (error) {
      log.error('[sync] importing new transactions failed most likely due to it already being in the DB');
      log.error(error);
    }

    try {
      await importTags(`${process.cwd()}/cache/tags.csv`);
    } catch (error) {
      log.error('[sync] importing new tags failed most likely due to it already being in the DB');
      log.error(error);
    }


    resetCacheStreams();

    if (!bar.complete) {
      bar.tick(batch.length);
    }

    SIGINT = false;

    if (SIGKILL === false) {
      await parallelize(height + batch.length);
    }
  }
}

export async function storeBlock(height: number, retry: number = 0) {
  try {
    const currentBlock = await block(height);
    const {formattedBlock, input} = serializeBlock(currentBlock, height);

    streams.block.cache.write(input);

    if (storeSnapshot) {
      streams.block.snapshot.write(input);
    }

    if (height > 0) {
      await storeTransactions(JSON.parse(formattedBlock.txs) as Array<string>, height);
    }
  } catch (error) {
    if (SIGKILL === false) {
      if (retry >= 25) {
        log.info(`[snapshot] there were problems retrieving ${height}, restarting the server`);
        await startSync();
      } else {
        log.info(`[snapshot] could not retrieve block at height ${height}, retrying`);
        await storeBlock(height, retry + 1);
      }
    }
  }
}

export async function storeTransactions(txs: Array<string>, height: number) {
  const batch = [];

  for (let i = 0; i < txs.length; i++) {
    const tx = txs[i];
    batch.push(storeTransaction(tx, height));
  }

  await Promise.all(batch);
}

export async function storeTransaction(tx: string, height: number, retry: boolean = true) {
  try {
    const currentTransaction = await transaction(tx);
    const {formattedTransaction, preservedTags, input} = serializeTransaction(currentTransaction, height);

    streams.transaction.cache.write(input);

    if (storeSnapshot) {
      streams.transaction.snapshot.write(input);
    }

    storeTags(formattedTransaction.id, preservedTags);

    const bundleType = tagValue(preservedTags, 'Bundle-Type');

    if (bundleType === 'ANS-102') {
      await processAns102(formattedTransaction.id, height);
    } else if (bundleType === 'ANS-104') {
      await processAns104(formattedTransaction.id, height);
    }
  } catch (error) {
    console.log('');
    log.info(`[database] could not retrieve tx ${tx} at height ${height} ${retry ? ', attempting to retrieve again' : ', missing tx stored in .rescan'}`);
    if (retry) {
      await storeTransaction(tx, height, false);
    } else {
      streams.rescan.cache.write(`${tx}|${height}|normal\n`);
      if (storeSnapshot) {
        streams.rescan.snapshot.write(`${tx}|${height}|normal\n`);
      }
    }
  }
}

/**
 * Processes an ANS-102 transaction and stores in a CSV stream
 * This will be stored in the database when the cron task is run
 *
 * @param {string} id - transaction id of the bundle
 * @param {number}  height - block height of transaction
 * @param {boolean}  retry - flag which describes whether to retry
 */
export async function processAns102(id: string, height: number, retry: boolean = true) {
  try {
    const ansPayload = await getDataFromChunks(id);
    const ansTxs = await ansBundles.unbundleData(ansPayload.toString('utf-8'));

    await cacheAns102Entries(ansTxs);
    await processAns102Transaction(ansTxs, height);
  } catch (error) {
    if (retry) {
      await processAns102(id, height, false);
    } else {
      log.info(`[database] malformed ANS payload at height ${height} for tx ${id}`);
      streams.rescan.cache.write(`${id}|${height}|ans\n`);
      if (storeSnapshot) {
        streams.rescan.snapshot.write(`${id}|${height}|ans\n`);
      }
    }
  }
}

/**
 * Processes an ANS-104 transaction and stores in a CSV stream
 * This will be stored in the database when the cron task is run
 *
 * @param {string} id - transaction id of the bundle
 * @param {number}  height - block height of transaction
 * @param {boolean}  retry - flag which describes whether to retry
 */
export async function processAns104(id: string, height: number, retry: boolean = true) {
  try {

  } catch (e) {

  }
}

export async function processAns102Transaction(ansTxs: Array<DataItemJson>, height: number) {
  for (let i = 0; i < ansTxs.length; i++) {
    const ansTx = ansTxs[i];
    const {ansTags, input} = serializeAnsTransaction(ansTx, height);

    streams.transaction.cache.write(input);

    if (storeSnapshot) {
      streams.transaction.snapshot.write(input);
    }

    for (let ii = 0; ii < ansTags.length; ii++) {
      const ansTag = ansTags[ii];
      const {name, value} = ansTag;

      const tag: DatabaseTag = {
        tx_id: ansTx.id,
        index: ii,
        name: name || '',
        value: value || '',
      };

      const input = `"${tag.tx_id}"|"${tag.index}"|"${tag.name}"|"${tag.value}"\n`;

      streams.tags.cache.write(input);

      if (storeSnapshot) {
        streams.tags.snapshot.write(input);
      }
    }
  }
}

export function storeTags(tx_id: string, tags: Array<Tag>) {
  for (let i = 0; i < tags.length; i++) {
    const tag = tags[i];
    const {input} = serializeTags(tx_id, i, tag);
    streams.tags.cache.write(input);
    if (storeSnapshot) {
      streams.tags.snapshot.write(input);
    }
  }
}


export function signalHook() {
  if (!TestSuite) {
    process.on('SIGINT', () => {
      log.info('[database] ensuring all blocks are stored before exit, you may see some extra output in console');
      SIGKILL = true;
      setInterval(() => {
        if (SIGINT === false) {
          log.info('[database] block sync state preserved, now exiting');
          process.exit();
        }
      }, 100);
    });
  }
}
