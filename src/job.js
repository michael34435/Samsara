/**
 * Module dependencies
 */
const _ = require('lodash');
const moment = require('moment');
const debug = require('debug')('Samsara:Job');

/**
 * Utilities
 */
const PubSub = require('../utils/pubsub');

/**
 * Configurations
 */

class Job {
  constructor({ name, data = {} }, config = {}) {
    this.name = name;
    this.data = data;
    this.config = config;

    this.pubsub = new PubSub(_.pick(config, ['credentials', 'projectId']));
  }

  async save() {
    const { topicSuffix, batching = {} } = this.config;
    const topicName = `${this.name}-${topicSuffix}`;
    const topic = await this.pubsub.createOrGetTopic(topicName);

    const dataBuffer = Buffer.from(
      JSON.stringify({
        topicName,
        createdAt: moment().utc(),
      }),
    );

    debug(`The job created on the ${topicName}`, { data: this.data, dataBuffer });

    return topic.publisher(batching).publish(dataBuffer, this.data);
  }
}

module.exports = Job;
