import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    for (const post of ops.posts.creates) {
      console.log(post.record.text)
    }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only alf-related posts
        return create.record.text.toLowerCase().includes('tari')
            || create.record.text.toLowerCase().includes('tarifanart')
            || create.record.text.toLowerCase().includes('metarunner')
            || create.record.text.toLowerCase().includes('smg4tari')
            || create.record.text.toLowerCase().includes('smg4tarifanart')
            || create.record.text.toLowerCase().includes('tarismg4')
            || create.record.text.toLowerCase().includes('tariplush')
            || create.record.text.toLowerCase().includes('tariplushy')
            || create.record.text.toLowerCase().includes('tariplushie')
            || create.record.text.toLowerCase().includes('metarunnertari')
            || create.record.text.toLowerCase().includes('tarimetarunner')
            || create.record.text.toLowerCase().includes('metarunnertarifanart')
      })
      .map((create) => {
        // map alf-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
