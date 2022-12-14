import { NextApiResponse, NextApiRequest } from "next";
import * as redis from "redis";

import Fetch from "src/utils/Fetch";
import { HTTP_METHOD } from "src/utils/constants";

const redisCache = new Promise<redis.RedisClient>((resolve, reject) => {
  const cache = redis.createClient(process.env.SNSCACHE_PORT, process.env.SNSCACHE_HOST, { no_ready_check: true });
  cache.auth(process.env.SNSCACHE_AUTH, err => {
    if (err) {
      reject(err);
    } else {
      resolve(cache);
    }
  });
});

const fetch = new Fetch(redisCache);

export default async function handler(req:NextApiRequest, res:NextApiResponse) {
  if ("GET" === req.method && "eligibility" === req.query.path) {
    const url = req.query.url as string;
    try {
      const cleanUrl = url.replace(/https{0,1}:\/\//, "");
      const hostname = new URL('https://' + cleanUrl).hostname;
      return fetch.request('https://' + cleanUrl, HTTP_METHOD.HEAD)
        .then(async ({ response }) => {
          if (response.headers["x-shopid"] || response.headers["x-sorting-hat-shopid"]) {
            const shopId = response.headers["x-shopid"] || response.headers["x-sorting-hat-shopid"];
            res.status(200).json({ "X-ShopId": shopId });
            // const shop = {
            //   "shop_id": shopId,
            //   "hostname": hostname,
            //   "limit": 250,
            //   "process_catalog": true,
            //   "process_variant_updates": true,
            //   "scheduler_key": `${new Date().getTime()}_JIT_${shopId}`,
            //   "jit": true
            // };
  
            // sqs.sendMessage({
            //   QueueUrl: "https://sqs.us-east-1.amazonaws.com/059332005338/commerceinspector-product-catalog-shop",
            //   MessageBody: JSON.stringify(shop)
            // }, (err) => {
            //     if (err) {
            //       res.status(404).json({ mesage: err.message, domain: hostname });
            //     } else {
            //       console.log(`Triggered JIT process for shop ${hostname}`);
            //       res.status(200).json(shop);
            //     }
            // });
            // const shopProcessorMessage = {
            //   scheduler_key: `${new Date().getTime()}_JITSHOP_${shopId}`,
            //   hostname,
            //   scrape_similarweb: hostname.endsWith("myshopify.com") || hostname.endsWith("shopifypreview.com") ? null : hostname,
            //   scrape_homepage: true,
            //   scrape_cartjs: true,
            // }
          
        } else {
          res.status(404).json({ error: `Site ${url} is not followable.` });
        }
      });
    } catch(err) {
      res.status(422).json({ error : `Site ${url} is not a valid hostanme.` });
    }
  } else {
    res.status(404).json({ error: "Not found." });
  }
}
