import { MongoClient } from "mongodb";
import { faker } from "@faker-js/faker";
import * as radash from "radash";

// Update MongoDB connection details
const DB_NAME = `bigwritedb`;
const MONGODB_URL = `mongodb://localhost:27017/${DB_NAME}?directConnection=true&ssl=false`;
const COLLECTION_NAME = `bigwritetest`;

const PARALLEL = 5; // number of parallel inserts to the DB
const NUMBER_OF_RECORDS = 125_000_000; // number of documents to be written to the DB

function data() {
    return {
        userid: faker.datatype.uuid(),
        avatar: faker.image.avatar(),
        birthday: faker.date.birthdate(),
        email: faker.internet.email(),
        firstName: faker.name.firstName(),
        lastName: faker.name.lastName(),
        sex: faker.name.sexType(),
        subscriptionTier: faker.helpers.arrayElement(['free', 'basic', 'business']),
    };
}

async function write() {
    console.log("started...", Date())
    try {
        const db = await MongoClient.connect(MONGODB_URL);
        const dbo = db.db(DB_NAME);
        const coll = await dbo.createCollection(COLLECTION_NAME);

        let ds = [];
        for (let i = 0; i < NUMBER_OF_RECORDS; i++) {
            ds.push(data());
            if (ds.length === PARALLEL) {
                await radash.parallel(PARALLEL, ds, async (d) => {
                    await coll.insertOne(d);
                });
                ds = [];
            }
        }
        db.close();   
    } catch (error) {
        console.error(error);
    }
    console.log("end...", Date())
}

write();