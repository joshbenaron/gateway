import * as Knex from 'knex';
import {config} from 'dotenv';

config();

export async function up(knex: Knex) {
  return knex.schema
      .withSchema(process.env.ENVIRONMENT || 'public')
      .createTable('transactions', (table) => {
        table.string('id', 64).notNullable();
        table.text('owner');
        table.jsonb('tags');
        table.string('target', 64);
        table.text('quantity');
        table.text('reward');
        table.text('signature');
        table.text('last_tx');
        table.bigInteger('data_size');
        table.string('content_type');
        table.integer('format', 2);
        table.integer('height', 4);
        table.string('owner_address');
        table.string('data_root', 64);
        table.string('parent', 64);
        table.timestamp('created_at').defaultTo(knex.fn.now());

        table.primary(['id'], 'pkey_transactions');
      })
      .createTable('blocks', (table) => {
        table.string('id', 64).notNullable();
        table.integer('height', 4).notNullable();
        table.timestamp('mined_at').notNullable();
        table.string('previous_block').notNullable();
        table.jsonb('txs').notNullable();
        table.jsonb('extended');
        table.timestamp('created_at').defaultTo(knex.fn.now());

        table.primary(['id'], 'pkey_blocks');
      })
      .createTable('tags', (table) => {
        table.string('tx_id', 64).notNullable();
        table.integer('index').notNullable();
        table.text('name');
        table.text('value');
        table.timestamp('created_at').defaultTo(knex.fn.now());

        table.primary(['tx_id', 'index'], 'pkey_tags');
      });
}

export async function down(knex: Knex) {
  return knex.schema
      .withSchema(process.env.ENVIRONMENT || 'public')
      .dropTableIfExists('transactions')
      .dropTableIfExists('blocks')
      .dropTableIfExists('tags');
}
