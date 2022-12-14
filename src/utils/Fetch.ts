"use strict";
import request from "request";
import _ from "underscore";
import { HTTP_METHOD, PROXY_PROVIDERS } from "./constants";
import FirehoseUtil from "./aws/FirehoseUtil";
import url from "url";
import querystring from "querystring";
import Bucket from "dripping-bucket";
import genericPool from "generic-pool";
import q from "q";
import { Context } from "aws-lambda";

const getMppProxiesEndpoint = (api_key: string): string => `https://api.myprivateproxy.net/v1/fetchProxies/json/full/${api_key}`;
const getRotateMppProxiesEndpoint = (api_key: string, proxy_plan_id: string): string => `https://api.myprivateproxy.net/v1/doRenew/${proxy_plan_id}/${api_key}`;
const getBlazingProxiesEndpoint = (email: string, key: string): string => `https://blazingseollc.com/proxy/dashboard/api/export/4/all/${email}/${key}/list.csv`;

export type Proxy = {
  proxy_ip: string;
  proxy_status: "online" | string;
  proxy_port: string; // this is a numeric string
  provider: string;
  username?: string;
  password?: string;
};

type ProxyLogRecord = {
  created_at: string; // timestamp (toISOString())
  hostname: string;
  pathname?: string;
  method: string;
  status_code?: number;
  runtime?: number;
  proxy_vendor?: string;
  proxy_ip_addr?: string;
  data?: { [key: string]: any };
};

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.143 YaBrowser/19.7.2.455 Yowser/2.5 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3724.8 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",
  "Mozilla/5.0 (X11; CrOS x86_64 12345.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3849.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3880.4 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3887.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3860.5 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.46 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/76.0.3809.87 Chrome/76.0.3809.87 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.143 YaBrowser/19.7.3.172 Yowser/2.5 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.56 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.56 Safari/537.36",
  "Mozilla/5.0 (X11; CrOS x86_64 12239.92.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.136 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36 AVG/75.1.849.144",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/76.0.3809.100 Chrome/76.0.3809.100 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3893.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3895.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
  "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) snap Chromium/76.0.3809.132 Chrome/76.0.3809.132 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.1.3029.81 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.35 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3710.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.19.2987.98 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.19 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.70 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3878.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3891.0 Safari/537.36",
  "Mozilla/5.0 (X11; CrOS x86_64 11895.118.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.159 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3893.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3894.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3896.6 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3895.5 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3223.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.56 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3888.1 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.52 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) snap Chromium/76.0.3809.87 Chrome/76.0.3809.87 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.145 Safari/537.36 Vivaldi/2.6.1566.49",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.0.1617 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3887.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.65 Safari/537.36",
  "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/73.0.3683.86 Chrome/73.0.3683.86 Safari/537.36",
  "Mozilla/5.0 (X11; CrOS x86_64 12371.46.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.63 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.42 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.42 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3237.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
  "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.19 (KHTML, like Gecko) Chrome/1.0.154.53 Safari/525.19",
  "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.19 (KHTML, like Gecko) Chrome/1.0.154.36 Safari/525.19",
  "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.10 (KHTML, like Gecko) Chrome/7.0.540.0 Safari/534.10",
  "Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.4 (KHTML, like Gecko) Chrome/6.0.481.0 Safari/534.4",
  "Mozilla/5.0 (Macintosh; U; Intel Mac OS X; en-US) AppleWebKit/533.4 (KHTML, like Gecko) Chrome/5.0.375.86 Safari/533.4",
  "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.2 (KHTML, like Gecko) Chrome/4.0.223.3 Safari/532.2",
  "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.201.1 Safari/532.0",
  "Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/3.0.195.27 Safari/532.0",
  "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/530.5 (KHTML, like Gecko) Chrome/2.0.173.1 Safari/530.5",
  "Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.10 (KHTML, like Gecko) Chrome/8.0.558.0 Safari/534.10",
  "Mozilla/5.0 (X11; U; Linux x86_64; en-US) AppleWebKit/540.0 (KHTML,like Gecko) Chrome/9.1.0.0 Safari/540.0",
  "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/9.0.600.0 Safari/534.14",
  "Mozilla/5.0 (X11; U; Windows NT 6; en-US) AppleWebKit/534.12 (KHTML, like Gecko) Chrome/9.0.587.0 Safari/534.12",
  "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.13 (KHTML, like Gecko) Chrome/9.0.597.0 Safari/534.13",
  "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.11 Safari/534.16",
  "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.20 (KHTML, like Gecko) Chrome/11.0.672.2 Safari/534.20",
  "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.792.0 Safari/535.1",
  "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.872.0 Safari/535.2",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7",
  "Mozilla/5.0 (Windows NT 6.0; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.66 Safari/535.11",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.45 Safari/535.19",
  "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
  "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
  "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.15 (KHTML, like Gecko) Chrome/24.0.1295.0 Safari/537.15",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1467.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1623.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.116 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.103 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.38 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36"
];

export default class Fetch {
  private readonly _cache: Promise<any>;

  private CACHE_KEY = "proxies";
  private CACHE_EXPIRY = 3600; //One hour in seconds
  private _proxies: Proxy[] = [];
  private readonly _bucket: Bucket; // the leaky bucket
  private _firehose: FirehoseUtil;
  private readonly _pool: any;

  private readonly MAX_REQUEST_TIME: number = process.env.MAX_FETCH_REQUEST_TIME ? Number(process.env.MAX_FETCH_REQUEST_TIME) : 10000;

  constructor(cache: any, maxReqs: number = 8) {
    this._cache = cache;
    this._firehose = new FirehoseUtil({
      region: process.env.SHINE_AWS_REGION,
      credentials: {
        accessKeyId: process.env.shine_prod_aws_access_key_id as string,
        secretAccessKey: process.env.shine_prod_aws_secret_key as string
      },
    });

    this._pool = genericPool.createPool(
      {
        create: async () => q.defer(),
        destroy: async () => { _.noop }
      },
      {
        max: maxReqs,
        min: 0,
        maxWaitingClients: 128,
        acquireTimeoutMillis: 10000
      }
    );

    // https://help.shopify.com/en/api/reference/rest-admin-api-rate-limits
    // the burst size is 40. if it's full, then i can execute 40 items over
    // the span of 20 seconds (2 per second) and i will only allow myself to
    // wait 10 seconds
    this._bucket = new Bucket({
      storage: {
        type: "memory"
      },
      buckets: {
        size: 40,
        refreshRate: 2,
        refreshInterval: 1
      },
      waitForTokenMs: 8 * 1000 // timeout after 8secs
    });
  }

  private getBlazingProxies = (): Promise<any> => {
    return new Promise(resolve => {
      if (!process.env.BP_EMAIL || !process.env.BP_KEY) {
        console.log("BlazingProxies email or key were not provided. Not getting proxies from them.");
        resolve([]);
        return;
      }

      const requestOptions = {
        uri: getBlazingProxiesEndpoint(process.env.BP_EMAIL, process.env.BP_KEY),
        method: HTTP_METHOD.GET
      };
      request(requestOptions, (err: any, response: any, body: any) => {
        if (err) {
          console.error("Something went wrong getting Blazing Proxies.", err);
          resolve([]);
          return;
        } else {
          if (!body) {
            console.error("No data from Blazing proxy endpoint.");
            resolve([]);
            return;
          }
          try {
            const data = body.split("\n");
            const results: Proxy[] = data.map((ip: string) => {
              const ipData = ip.split(":");
              return {
                proxy_ip: ipData[0],
                proxy_status: "online",
                proxy_port: ipData[1],
                provider: PROXY_PROVIDERS.BP,
                username: ipData[2],
                password: ipData[3]
              };
            });
            resolve(results);
          } catch (err) {
            console.error("Issue with parsing proxy body", err);
            resolve([]);
          }
        }
      });
    });
  };

  private validateProxies = (): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (this._proxies.length) {
        resolve();
      } else {
        this._cache
          .then(
            cache =>
              new Promise((resolve, reject) => {
                cache.get(this.CACHE_KEY, (err, reply) => (err || !reply ? reject() : resolve(reply)));
              })
          )
          .then((proxies: string) => {
            const data: Proxy[] = JSON.parse(proxies);
            this._proxies = data;
            resolve();
            return;
          })
          .catch(async () => {
            try {
              const proxies = await this.getBlazingProxies();
              this._proxies = this._proxies.length ? this._proxies : proxies;
              this._cache.then(cache => {
                cache.set(this.CACHE_KEY, JSON.stringify(this._proxies), "EX", this.CACHE_EXPIRY);
              });
              resolve();
            } catch (err) {
              reject(err);
            }
          });
      }
    });
  };

  private getProxy = (key?: string): Proxy | null => {
    if (!this._proxies.length) {
      return null;
    }

    let proxy: Proxy;
    if (key) {
      const hashedValue = this.hashCode(key);
      const hashedIndex = hashedValue % this._proxies.length;
      proxy = this._proxies[hashedIndex];
    } else {
      proxy = this._proxies[Math.floor(Math.random() * this._proxies.length)];
    }
    return proxy;
  };

  private getUserAgent = (key?: string): string => {
    let result: string;
    if (key) {
      const hashedValue = this.hashCode(key);
      const hashedIndex = hashedValue % USER_AGENTS.length;
      result = USER_AGENTS[hashedIndex];
    } else {
      result = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    }
    return result;
  };

  private getProxyUrl = proxy => {
    return proxy.username && proxy.password ? `http://${proxy.username}:${proxy.password}@${proxy.proxy_ip}:${proxy.proxy_port}` : `http://${proxy.proxy_ip}:${proxy.proxy_port}`;
  };

  /**
   * https://werxltd.com/wp/2010/05/13/javascript-implementation-of-javas-string-hashcode-method/
   */
  private hashCode = (str: string) => {
    var hash = 0,
      i,
      chr;
    if (str.length === 0) return hash;
    for (i = 0; i < str.length; i++) {
      chr = str.charCodeAt(i);
      hash = (hash << 5) - hash + chr;
      hash |= 0; // Convert to 32bit integer
    }
    return Math.abs(hash);
  };

  public request = (uri: string, httpMethod: HTTP_METHOD, shouldUseProxy: boolean = true, customOptions?: {}, key?: string, context?: Context): Promise<any> => {
    const reqUrl = url.parse(uri);
    return new Promise(async (resolve, reject) => {
      let def: any = null;
      const start = new Date();
      try {
        def = await this._pool.acquire();
      } catch (err) {
        reject(err);
        const endTime = new Date().getTime();
        const lambda = context ? { request_id: context.awsRequestId } : undefined;
        const rand = Math.floor(Math.random() * 10);
        if (rand === 0) {
          this._firehose.postRecord(
            {
              created_at: start.toISOString(),
              hostname: reqUrl.hostname || "",
              pathname: reqUrl.pathname,
              method: httpMethod,
              status_code: -509,
              runtime: endTime - start.getTime(),
              data: {
                lambda,
                error: err.toString && "function" === typeof err.toString ? err.toString() : "could not acquire resource from pool"
              }
            },
            "commerceinspector_proxy_log"
          );
        }
      }
      if (null !== def) {
        def.promise.then(() => this._pool.release(def)).catch(() => this._pool.release(def));
        let p = this.doRequest(reqUrl, httpMethod, shouldUseProxy, customOptions, key, context)
          .then(result => {
            def.resolve(result);
            resolve(result);
          })
          .catch(err => {
            def.reject(err);
            reject(err);
          });
      }
    });
  };

  private doRequest = (reqUrl: url.UrlWithStringQuery, httpMethod: HTTP_METHOD, shouldUseProxy: boolean, customOptions?: any, key?: string, context?: Context): Promise<any> => {
    let requestComplete: boolean = false;
    let timeout: NodeJS.Timeout | null = null;
    let lambda = context ? { request_id: context.awsRequestId } : undefined;
    return new Promise(async (resolve, reject) => {
      let record: ProxyLogRecord | null = null;
      let start: Date | null = null;
      let delay: number = 0;
      const defaultRequestHeaders = {
        "User-Agent": this.getUserAgent(key),
        "Cache-Control": "no-cache"
      };
      const requestOptions = {
        uri: reqUrl.href,
        method: httpMethod,
        headers: {
          ...defaultRequestHeaders,
          ...(customOptions && customOptions.headers && { ...customOptions.headers })
        },
        ...(customOptions && _.omit(customOptions, "headers"))
      };
      start = new Date();
      record = {
        created_at: start.toISOString(),
        hostname: reqUrl.hostname || "",
        pathname: reqUrl.pathname as string,
        method: httpMethod,
        data: {
          lambda,

          // add querystring to data object (if present)
          ...(reqUrl.query ? { query: querystring.parse(reqUrl.query) } : {}),

          // add requestHeaders to data object (if present)
          ...(requestOptions && requestOptions.headers
            ? {
                requestHeaders: requestOptions.headers
              }
            : {})
        }
      };
      if (shouldUseProxy) {
        try {
          await this.validateProxies();
          const proxy = this.getProxy(key);
          if (!proxy) {
            throw Error();
          }
          if (record) {
            record.proxy_ip_addr = `${proxy.proxy_ip}:${proxy.proxy_port}`;
            record.proxy_vendor = proxy.provider;
          }
          requestOptions["proxy"] = this.getProxyUrl(proxy);
          delay = await this._bucket.getDelay(requestOptions["proxy"]);
        } catch (err) {
          if (record) {
            record.status_code = -412;
            record.data = { ...record.data, error: "error setting up proxy" };
            const rand = Math.floor(Math.random() * 10);
            if (rand === 0) {
              this._firehose
                .postRecord(record, "commerceinspector_proxy_log")
                .then(_.noop)
                .catch(err => console.error("Something went wrong with posting to firehose", err));
            }
          }
          console.error("Something went wrong with using proxies", err);
          reject("Unable to use proxy to make request");
          return;
        }
      }
      if (!delay) {
        const req = request(requestOptions, (err: any, response: any, body: any) => {
          let endTime = new Date().getTime();
          if (null !== timeout) {
            clearTimeout(timeout);
          }
          if (err) {
            console.error("Error making request.", err);
            reject(err);
            if (record && start !== null) {
              record.runtime = endTime - start.getTime();
              record.data = {
                ...record.data,
                error: err.message || "unknown error making request"
              };
              const rand = Math.floor(Math.random() * 10);
              if (rand === 0) {
                this._firehose
                  .postRecord(record, "commerceinspector_proxy_log")
                  .then(_.noop)
                  .catch(console.log);
              }
            }
          } else {
            resolve({ response, body });
            if (record && start !== null) {
              record.runtime = endTime - start.getTime();
              record.status_code = response.statusCode;
              record.data = {
                ...record.data,

                // add response headers to data object
                responseHeaders: response.headers,

                // add the response body in for non-200 responses
                ...(200 === response.statusCode
                  ? {}
                  : {
                      responseBody: body
                    })
              };
              const rand = Math.floor(Math.random() * 10);
              if (rand === 0) {
                this._firehose
                  .postRecord(record, "commerceinspector_proxy_log")
                  .then(_.noop)
                  .catch(console.log);
              }
            }
          }
          if (shouldUseProxy && requestOptions["proxy"]) {
            requestComplete = true;
            this._bucket.returnToken(requestOptions["proxy"]);
          }
        });

        if (shouldUseProxy) {
          timeout = setTimeout(() => {
            req.abort();
            const err = `Request has reached the max request time: ${this.MAX_REQUEST_TIME}. Forcing abort`;
            if (record && start !== null) {
              record.runtime = this.MAX_REQUEST_TIME;
              record.data = {
                ...record.data,
                error: err
              };
              record.status_code = -408;
              const rand = Math.floor(Math.random() * 10);
              if (rand === 0) {
                this._firehose
                  .postRecord(record, "commerceinspector_proxy_log")
                  .then(_.noop)
                  .catch(console.log);
              }
            }
            reject(err);
            if (requestOptions["proxy"] && !requestComplete) {
              requestComplete = true;
              this._bucket.returnToken(requestOptions["proxy"]);
            }
          }, this.MAX_REQUEST_TIME);
        }
      } else {
        // the leaky bucket says to delay this work. instead, we should just throw
        if (record) {
          record.status_code = -429;
          record.data = { error: "too many requests", ...record.data };
          const rand = Math.floor(Math.random() * 10);
          if (rand === 0) {
            this._firehose
              .postRecord(record, "commerceinspector_proxy_log")
              .then(_.noop)
              .catch(console.log);
          }
        }
        reject({ ...record, data: record.data });
      }
    });
  };
}
