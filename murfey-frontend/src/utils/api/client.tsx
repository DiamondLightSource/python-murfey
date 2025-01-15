import { createStandaloneToast } from "@chakra-ui/toast";
import { baseToast } from "@diamondlightsource/ui-components";
const { toast } = createStandaloneToast();

const controller = new AbortController();

const defaultSettings: Partial<RequestConfig> = {
  credentials:
    process.env.NODE_ENV === "development" ? "include" : "same-origin",
};

interface RequestConfig {
  method: string;
  headers: Record<string, string>;
  body?: string | FormData;
  [k: string]: any;
}

export interface Response {
  status: number;
  data: any;
  headers: Record<string, any>;
  url: string;
}

export const getPrefix = (prefix: string = "/api/") => {
  if (prefix.substring(0, 1) === "/") {
    return window.location.origin + prefix;
  }

  return prefix;
};

export const client = async (
  endpoint: string,
  customConfig: Record<any, any> = {},
  body?: Record<any, any> | FormData | null,
  method: string | null = null,
  errToast: boolean = true,
  prefix: string = getPrefix(
    sessionStorage.getItem("murfeyServerURL") ??
      process.env.REACT_APP_API_ENDPOINT,
  ),
): Promise<never | Response> => {
  const config: RequestConfig = {
    method: method ? method : body != null ? "POST" : "GET",
    ...customConfig,
    headers: {
      ...customConfig.headers,
    },
    signal: controller.signal,
    body: undefined,
    ...defaultSettings,
  };
  console.log(endpoint);

  if (body != null) {
    if (!(body instanceof FormData)) {
      config.body = JSON.stringify(body);
      config.headers = {
        ...config.headers,
        Accept: "application/json",
        "Content-Type": "application/json",
      };
    } else {
      config.body = body;
    }
  }

  try {
    const token = sessionStorage.getItem("token");
    config.headers = { ...config.headers, Authorization: `Bearer ${token}` };
    const response = await fetch(prefix + endpoint, config);
    const isJson = response.headers.get("content-type") === "application/json";

    if (!isJson) console.log(endpoint);

    return {
      status: response.status,
      data: isJson ? await response.json() : await response.arrayBuffer(),
      headers: response.headers,
      url: response.url,
    };
  } catch (err) {
    if (!toast.isActive("main-toast") && errToast) {
      toast({
        ...baseToast,
        title:
          "An error has occurred while fetching data, please try again later.",
        status: "error",
      });
    }

    throw err;
  }
};

client.get = async (
  endpoint: string,
  customConfig = {},
  errToast: boolean = true,
) => {
  return await client(
    endpoint,
    (customConfig = {
      ...customConfig,
    }),
    null,
    "GET",
    (errToast = errToast),
  );
};

client.hub_get = async (
  endpoint: string,
  customConfig = {},
  errToast: boolean = true,
) => {
  return await client(
    endpoint,
    (customConfig = {
      ...customConfig,
    }),
    null,
    "GET",
    (errToast = errToast),
    getPrefix(process.env.REACT_APP_HUB_ENDPOINT),
  );
};

client.delete = async (endpoint: string, customConfig = {}) => {
  return await client(
    endpoint,
    (customConfig = {
      ...customConfig,
    }),
    null,
    "DELETE",
  );
};

client.post = async (
  endpoint: string,
  body: Record<any, any> | FormData,
  customConfig = {},
) => {
  return await client(endpoint, { ...customConfig }, body);
};

client.put = async (
  endpoint: string,
  body: Record<any, any> | FormData,
  customConfig = {},
) => {
  return await client(endpoint, { ...customConfig }, body, "PUT");
};

export const prependApiUrl = (url: string) =>
  `${getPrefix(
    sessionStorage.getItem("murfeyServerURL") ??
      process.env.REACT_APP_API_ENDPOINT,
  )}${url}`;
