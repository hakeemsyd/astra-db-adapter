/**
 * <div style={{display: "flex", justifyContent: "space-between", alignItems: "center", padding: 16}}>
 *  <p style={{fontWeight: "normal"}}>Official <a href="https://astra.datastax.com/">Astra DB</a> adapter for Auth.js / NextAuth.js.</p>
 *  <a href="https://astra.datastax.com/">
 *   <img style={{display: "block"}} src="https://authjs.dev/img/adapters/astra-db.png" width="48" />
 *  </a>
 * </div>
 *
 * ## Installation
 *
 * ```bash npm2yarn2pnpm
 * npm install @auth/astra-db-adapter
 * ```
 *
 * @module @auth/astra-db-adapter
 */

/**
 * ## Setup
 *
 * Require the mentioned collections in the astra Database:
 * "users"
 * "sessions"
 * "accounts"
 * "verificationTokens"
 *
 * How to create the required collections:
 * By Using this bash script.
 * paste it in a file_name.sh
 * make sure to make it executable by chmod +x file_name.sh
 * then run the bash script by ./file_name.sh
 *
 * ```bash
 * #!/bin/bash
 * export ASTRA_DB_ID="YOUR_ASTRA_DB_ID"
 * export ASTRA_DB_REGION="YOUR_ASTRA_DB_REGION"
 * export ASTRA_DB_KEYSPACE="YOUR_ASTRA_DB_KEYSPACE"
 * export ASTRA_DB_APPLICATION_TOKEN="YOUR_ASTRA_DB_APPLICATION_TOKEN"
 * export ASTRA_URL="https://$ASTRA_DB_ID-$ASTRA_DB_REGION.apps.astra.datastax.com/api/json/v1/$ASTRA_DB_KEYSPACE"
 * collections=("users" "accounts" "sessions" "verificationTokens")
 * for collection in "${collections[@]}"
 * do
 *   curl -X POST "$ASTRA_URL" \
 *       -H "x-cassandra-token: $ASTRA_DB_APPLICATION_TOKEN" \
 *       -H "Content-Type: application/json" \
 *       -d "{\"createCollection\": {\"name\": \"$collection\"}}"
 * done
 * ```
 *
 * ### required environment variables:
 * ASTRA_DB_ID
 * ASTRA_DB_REGION
 * ASTRA_DB_KEYSPACE
 * ASTRA_DB_APPLICATION_TOKEN
 */

import type {
  Adapter,
  AdapterAccount,
  AdapterSession,
  AdapterUser,
  VerificationToken,
} from "@auth/core/src/adapters"

export interface AstraDBConfig {
  collections?: {
    users?: string
    sessions?: string
    accounts?: string
    verificationTokens?: string
  }
  api: {
    dbId: string
    region: string
    keyspace: string
    token: string
  }
}

export const defaultCollections = {
  users: "users",
  sessions: "sessions",
  accounts: "accounts",
  verificationTokens: "verificationTokens",
} satisfies AstraDBConfig["collections"]

interface AstraResponse<T> {
  data?: { document: T | null }
  errors: { message: string; errorCode: string }[]
  status: any
}

// https://github.com/honeinc/is-iso-date/blob/master/index.js
const isoDateRE =
  /(\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z))|(\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z))|(\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z))/

function isDate(value: unknown): value is string | number {
  if (typeof value !== "string") return false
  return isoDateRE.test(value) && !isNaN(Date.parse(value))
}

export const format = {
  /** Takes a DB response and returns a plain old JavaScript object */
  from<T = Record<string, unknown>>(
    object: AstraResponse<T>,
    includeId: boolean = true
  ): T | null {
    if (object.errors?.length) {
      const e = new Error(object.errors[0].message)
      e.cause = object.errors
      throw e
    }

    if (!object.data?.document) return null

    const newObject: Record<string, unknown> = {}
    for (const key in object.data.document) {
      const value = object.data.document[key]
      if (key === "_id") newObject["id"] = value
      else if (isDate(value)) newObject[key] = new Date(value)
      else newObject[key] = value
    }
    if (!includeId) delete newObject.id
    return newObject as T
  },
}

/** Fetch data from the DataStax API */
function fetchClient(api: AstraDBConfig["api"]) {
  const baseUrl = `https://${api.dbId}-${api.region}.apps.astra.datastax.com/api/json/v1/${api.keyspace}`
  return {
    request(collection: string, data: any) {
      const url = new URL(`${baseUrl}/${collection}`)
      return fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-cassandra-token": api.token,
        },
        body: JSON.stringify(data),
      })
        .then((res) => res.json())
        .catch((error) => {
          console.error(error)
          throw new Error("TODO: Handle errors")
        })
    },
  }
}

export function AstraDBAdapter(config: AstraDBConfig): Adapter {
  const { api } = config
  const collections = { ...defaultCollections, ...config.collections }
  const { users, accounts, sessions, verificationTokens: tokens } = collections
  const client = fetchClient(api)

  return {
    async createUser(user) {
      return format.from(
        await client.request(users, {
          findOneAndUpdate: {
            filter: { email: user.email },
            update: { $set: user },
            options: { returnDocument: "after", upsert: true },
          },
        })
      )!
    },
    async getUser(id) {
      return format.from(
        await client.request(users, { findOne: { filter: { id } } })
      )
    },
    async getUserByEmail(email) {
      return format.from(
        await client.request(users, { findOne: { filter: { email } } })
      )
    },
    async updateUser(user) {
      const { id: id, ...rest } = user
      return format.from(
        await client.request(users, {
          findOneAndUpdate: {
            filter: { id },
            update: { $set: rest },
            options: { returnDocument: "after", upsert: false },
          },
        })
      )!
    },
    async createSession(document) {
      const { status } = await client.request(sessions, {
        insertOne: { document },
      })
      return { ...document, id: status.insertedId }
    },
    async getSessionAndUser(sessionToken) {
      const session = format.from<AdapterSession>(
        await client.request(sessions, {
          findOne: { filter: { sessionToken } },
        })
      )
      if (!session) return null

      const user = format.from<AdapterUser>(
        await client.request(users, {
          findOne: { filter: { id: session.userId } },
        })
      )

      if (!user) return null

      return { session, user }
    },
    async updateSession(session) {
      const { sessionToken } = session
      return format.from(
        await client.request(sessions, {
          findOneAndUpdate: {
            filter: { sessionToken },
            update: { $set: session },
            options: { returnDocument: "after", upsert: false },
          },
        })
      )
    },
    async deleteSession(sessionToken) {
      const requests = [
        client.request(sessions, { findOne: { filter: { sessionToken } } }),
        client.request(sessions, { deleteOne: { filter: { sessionToken } } }),
      ]

      return (
        format.from<AdapterSession>((await Promise.all(requests))[0]) ?? null
      )
    },
    async createVerificationToken(document) {
      await client.request(tokens, { insertOne: { document } })
      return document
    },
    async useVerificationToken(filter) {
      const requests = [
        client.request(tokens, { findOne: { filter } }),
        client.request(tokens, { deleteMany: { filter } }),
      ]
      return format.from<VerificationToken>(
        (await Promise.all(requests))[0],
        false
      )
    },
    async linkAccount(document) {
      const { status } = await client.request(accounts, {
        insertOne: { document },
      })
      return { ...document, id: status.insertedId }
    },
    async getUserByAccount(filter) {
      const account = format.from(
        await client.request(accounts, { findOne: { filter } })
      )
      if (!account) return null

      return format.from(
        await client.request(users, {
          findOne: { filter: { id: account.userId } },
        })
      )
    },
    async unlinkAccount(filter) {
      const requests = [
        client.request(accounts, { findOne: { filter } }),
        client.request(accounts, { deleteMany: { filter } }),
      ]
      return format.from<AdapterAccount>((await Promise.all(requests))[0])!
    },
    async deleteUser(userId) {
      const requests = [
        client.request(users, { deleteMany: { filter: { id: userId } } }),
        client.request(accounts, { deleteMany: { filter: { userId } } }),
        client.request(sessions, { deleteMany: { filter: { userId } } }),
      ]
      return format.from<AdapterUser>((await Promise.all(requests))[0])
    },
  }
}
