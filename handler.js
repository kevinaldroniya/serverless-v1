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

const saveToRedis = async (obj, prefix = "service_data::") => {
  for (const [key, value] of Object.entries(obj)) {
    const redisKey = `${prefix}${key}`;
    if (typeof value === "object" && !Array.isArray(value)) {
      await redis.set(redisKey, JSON.stringify(value));
      await saveToRedis(value, `${redisKey}.`);
    } else {
      await redis.set(redisKey, JSON.stringify(value));
    }
  }
};

// Helper function to generate keys
const generateKeyName = (serviceName) => {
  return "service_data::" + serviceName;
};

// Lambda function to add data to S3
module.exports.add = async (event) => {
  const newData = JSON.parse(event.body);
  let existingData = {};

  try {
    const params = {
      Bucket: BUCKET_NAME,
      Key: FILE_KEY,
    };
    const s3Response = await s3.getObject(params).promise();
    existingData = JSON.parse(s3Response.Body.toString());
  } catch (error) {
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
  existingData = { ...existingData, ...newData };

  try {
    // Upload JSON to S3
    await s3
      .putObject({
        Bucket: BUCKET_NAME,
        Key: FILE_KEY,
        Body: JSON.stringify(existingData, null, 2),
        ContentType: "application/json",
      })
      .promise();

    // Cache data to redis
    await saveToRedis(existingData);
    // for (const [serviceName, serviceData] of Object.entries(existingData)) {
    //   const redisKey = generateKeyName(serviceName);
    //   const dataValue =
    //     typeof serviceData === "object"
    //       ? JSON.stringify(serviceData)
    //       : serviceData;
    //   await redis.set(redisKey, dataValue);
    // }

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
  const { key } = event.queryStringParameters;
  const partPath = key.split(".");
  const redisKey = generateKeyName(key);

  try {
    // Check redis first
    const cachedData = await redis.get(redisKey);
    if (cachedData) {
      let parsedData;
      try {
        parsedData = JSON.parse(cachedData);
      } catch (e) {
        parsedData = cachedData; // Use as-is if parsing fails
      }

      return {
        statusCode: 200,
        body: JSON.stringify({ [key]: parsedData }),
      };
    }

    // if not found in redis, check on s3
    const s3Data = await s3
      .getObject({
        Bucket: BUCKET_NAME,
        Key: FILE_KEY,
      })
      .promise();
    const data = JSON.parse(s3Data.Body.toString());
    const result = partPath.reduce((acc, part) => {
      if (acc && acc[part] !== undefined) {
        return acc[part];
      }else{
        return undefined;
      }
    }, data);

    if (!result) {
      return {
        statusCode: 404,
        body: JSON.stringify({
          error: `Key "${key}" not found in data`,
        }),
      };
    }
    

    // Cache data in redis
    await saveToRedis(data);
    // const dataValue =
    //   typeof data[partPath[0]] === "object"
    //     ? JSON.stringify(data[partPath[0]])
    //     : data[partPath[0]];
    // console.log("🚀 ~ module.exports.read= ~ dataValue:", dataValue);

    // await redis.set(redisKey, dataValue);
    return {
      statusCode: 200,
      body: JSON.stringify({
        [key]: result,
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
