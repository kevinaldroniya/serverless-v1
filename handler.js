const AWS = require("aws-sdk");
const Redis = require("ioredis");

AWS.config.update({ logger: console });
// Configure AWS SDK to use localstack
const s3 = new AWS.S3({
  endpoint: "http://172.17.0.3:4566", // localstack endpoint
  s3ForcePathStyle: true,
  signatureVersion: "v4",
});

// Connect to redis
const redis = new Redis({
  host: "172.17.0.2", // redis host
  port: 6379, // redis port
});

exports.hello = async (event) => {
  return {
    statusCode: 200,
    body: JSON.stringify({
      message: "Go Serverless v4! Your function executed successfully!",
    }),
  };
};

const BUCKET_NAME = "my-local-bucket";
const FILE_KEY = "service_data.json";

async function cacheToRedis(data) {
  try {
    const pipeline = redis.pipeline();
    Object.entries(data).forEach(([key, value]) => {
      pipeline.set(generateKeyName(key), JSON.stringify(value));
    });
    await pipeline.exec();
  } catch (error) {
    console.error("Redis Cache Error:", error);
  }
}

async function fetchFromRedis(key) {
  try {
    const data = await redis.get(generateKeyName(key));
    return data ? JSON.parse(data) : null;
  } catch (error) {
    console.error("Redis Fetch Error:", error.message);
    return null; // Fail gracefully and let S3 handle the request
  }
}

function getNestedValue(obj, path) {
  return path.reduce((acc, part) => {
    if (acc && typeof acc === "object") return acc[part];
    return undefined;
  }, obj);
}

async function s3FileExists(bucket, key) {
  try {
    await s3.headObject({ Bucket: bucket, Key: key }).promise();
    return true;
  } catch (error) {
    return error.code === "NotFound" ? false : true;
  }
}

async function saveDataToS3(data) {
  await s3
    .putObject({
      Bucket: BUCKET_NAME,
      Key: FILE_KEY,
      Body: JSON.stringify(data, null, 2),
      ContentType: "application/json",
    })
    .promise();
}

// Helper function to generate keys
const generateKeyName = (serviceName) => {
  return "service_data::" + serviceName;
};

// Lambda function to add data to S3
module.exports.add = async (event) => {
  let newData;
  let existingData = {};
  try {
    newData = JSON.parse(event.body);
  } catch (error) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        error: "Invalid JSON format",
      }),
    };
  }

  try {
    if (await s3FileExists(BUCKET_NAME, FILE_KEY)) {
      const s3Response = await s3
        .getObject({ Bucket: BUCKET_NAME, Key: FILE_KEY })
        .promise();
      existingData = JSON.parse(s3Response.Body.toString());
    }
  } catch (error) {
    console.error("S3 Fetch Error:", error);
    if (error.code !== "NoSuchKey") {
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Failed to fetch existing data",
          details: error.message,
        }),
      };
    }
  }

  // Merge existing data with the new one
  existingData = Object.assign({}, existingData, newData);

  try {
    // Upload JSON to S3
    await saveDataToS3(existingData);

    // Cache data to redis
    await cacheToRedis(existingData);
    return {
      statusCode: 201,
      body: JSON.stringify({
        message: "Data saved successfully",
      }),
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: "Failed to save data",
        details: error.message,
      }),
    };
  }
};

module.exports.read = async (event) => {
  try {
    if (!event.queryStringParameters?.key?.trim()) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing 'key' parameter" }),
      };
    }
    const { key } = event.queryStringParameters;
    const partPath = key.split(".");
    const redisKey = generateKeyName(partPath[0]);

    let result = await fetchFromRedis(redisKey);

    if (!result) {
      if (!(await s3FileExists(BUCKET_NAME, FILE_KEY))) {
        return {
          statusCode: 404,
          body: JSON.stringify({
            error: `Key "${key}" not found in data`,
          }),
        };
      }
      const s3Response = await s3
        .getObject({ Bucket: BUCKET_NAME, Key: FILE_KEY })
        .promise();
      const data = JSON.parse(s3Response.Body.toString());
      result = data[partPath[0]];

      if (result) await cacheToRedis({ [partPath[0]]: result });
    }

    const finalResult = getNestedValue(result, partPath.slice(1));

    return finalResult !== undefined
      ? { statusCode: 200, body: JSON.stringify({ [key]: finalResult }) }
      : {
          statusCode: 404,
          body: JSON.stringify({
            error: `Key "${key}" not found in data`,
          }),
        };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: "Failed to retrieve data",
        details: error.message,
      }),
    };
  }
};

module.exports.delete = async (event) => {
  if (!event.queryStringParameters?.key?.trim()) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: "Missing 'key' parameter" }),
    };
  }

  const { key } = event.queryStringParameters;
  const redisKey = generateKeyName(key);

  try {
    await redis.del(redisKey);
    if (!(await s3FileExists(BUCKET_NAME, FILE_KEY))) {
      return {
        statusCode: 404,
        body: JSON.stringify({
          error: `Key "${key}" not found in data`,
        }),
      };
    }
    const s3Response = await s3
      .getObject({ Bucket: BUCKET_NAME, Key: FILE_KEY })
      .promise();
    let existingData = JSON.parse(s3Response.Body.toString());

    // Remove key from json data
    if (!(key in existingData)) {
      return {
        statusCode: 404,
        body: JSON.stringify({ error: `Key "${key}" not found in S3 data` }),
      };
    }

    delete existingData[key];

    const updatedData =
      Object.keys(existingData).length > 0 ? existingData : {};

    await saveDataToS3(updatedData);

    return {
      statusCode: 200,
      body: JSON.stringify({ message: `Key "${key}" deleted successfully` }),
    };
  } catch (error) {
    console.error("Delete Error:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: "Failed to delete key",
        details: error.message,
      }),
    };
  }
};

module.exports.clearCache = async () => {
  try {
    let cursor = 0;
    do {
      const scanResult = await redis.scan(
        cursor,
        "MATCH",
        "service_data::*",
        "COUNT",
        100
      );
      cursor = scanResult[0];
      const keysToDelete = scanResult[1];
      if (keysToDelete.length > 0) {
        await redis.del(...keysToDelete);
      }
    } while (cursor !== "0");
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "All service_data cache cleared successfully",
      }),
    };
  } catch (error) {
    console.error("Redis Clear Cache Error:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: "Failed to clear Redis cache",
        details: error.message,
      }),
    };
  }
};
