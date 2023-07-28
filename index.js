import dotenv from 'dotenv'
dotenv.config();
import { MongoClient } from "mongodb"
import { ListTablesCommand, DynamoDBClient, ScanCommand  } from "@aws-sdk/client-dynamodb"
import { unmarshall } from "@aws-sdk/util-dynamodb"

const dynamoDbClient = new DynamoDBClient({region: process.env.REGION_AWS});

const mongoDbClient = new MongoClient(process.env.URL_MONGO);
const mongoDbDatabase = mongoDbClient.db(process.env.DATABASE_MONGO);

async function getAllTablesDynamoDB() {
    const command = new ListTablesCommand({});
    const response = await dynamoDbClient.send(command);

    return response.TableNames;
}

async function getAllDataDynamoDB(table) {
    const command = new ScanCommand({
        TableName: table
      });

    console.log(`Scanning table ${table}`);

    let lastEvaluatedKey = "dummy";
    const items = []
    while (lastEvaluatedKey) {
        const data = await dynamoDbClient.send(command);
        
        items.push(...data.Items);
        lastEvaluatedKey = data.LastEvaluatedKey;

        if (lastEvaluatedKey) {
            command.input.ExclusiveStartKey = lastEvaluatedKey;
        }
    }

    return items.map(x => unmarshall(x));
}

async function insertMongoDb(collectionName, items) {
    const collection = mongoDbDatabase.collection(collectionName);
    try {
        await collection.drop();
    } catch {}
    
    const options = { ordered: true };
    const result = await collection.insertMany(items, options);

    return result;
}

async function run() {
    try {

        const tablesName = await getAllTablesDynamoDB();
        
        for (const table of tablesName) {
            const items = await getAllDataDynamoDB(table);

            if(items.length <= 0) continue; 

            const resultInsert = await insertMongoDb(table, items);

            console.log(`${resultInsert.insertedCount} documents were inserted in ${table} collection.`);
        }
    } finally {
      await mongoDbClient.close();
    }
  }

  run().catch(console.dir);