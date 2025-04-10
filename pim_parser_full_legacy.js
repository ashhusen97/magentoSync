const Typesense = require("typesense");
const axios = require("axios");
const express = require("express");
const bodyParser = require("body-parser");
const app = express();
const PORT = 3000;
const fs = require("fs");
const path = require("path");

// Middleware to parse JSON request body
app.use(bodyParser.json());
let DOC_REPO = "products_en-US_v8";
const MAG_TOKEN = "j0l7it2zcd7u2c91qtli4tcwy8o1kvpl";
const TYPESENSE_CONFIG = {
  nodes: [
    {
      host: "canada.paperhouse.com",
      port: 8108,
      path: "",
      protocol: "http",
    },
  ],
  apiKey: "1eDMDj6SeFmrTTwUGprBxEGjF4aGfK59Pt0j36fr1lkF8BO2",
};

const typesense = new Typesense.Client(TYPESENSE_CONFIG);

const sales_data = [];
let sales_data_status = "";

const getGQProduct = async (id) => {
  const endpoint = "https://www.foodservicedirect.com/graphql";
  const headers = {
    "content-type": "application/json",
  };
  const graphqlQuery = {
    query: `{
      products :product_details(skus_comma_separated:"${id},${Date.now()}"){
            sku
            vendor_id
            image
            name
            price
            url
            ships_in
            is_in_stock
            qty
            liquidation
            liquidation_expiry_date
            new_arrivals
            new_arrivals_expiry_date
            rebate_eligibility
            product_review{
              review_count
              rating_avg
            }
            sold_as
            shipping_type
            ships_in_days
            mp_special_price
            mp_special_from_date
            mp_special_to_date
            stock_type
          }
        }`,
  };
  const response = await axios({
    url: endpoint,
    method: "post",
    headers: headers,
    data: graphqlQuery,
  });
  const product = response?.data?.data?.products?.[0] || {};

  return product;
};

const getMagProduct = async (id) => {
  const config = {
    headers: { Authorization: "bearer " + MAG_TOKEN },
  };
  const response = await axios.get(
    `https://www.foodservicedirect.com/rest/default/V1/products/${id}`,
    config
  );
  const product = response.data;

  return product;
};

const getMagProductFSD = async (id) => {
  const config = {
    headers: { Authorization: "bearer " + MAG_TOKEN },
  };
  const response = await axios.get(
    `https://www.foodservicedirect.com/rest/V1/fsd/product/${id}`,
    config
  );
  const product = response.data[0];

  return product;
};

const getMagProducInfo = async (id) => {
  const config = {
    headers: { Authorization: "bearer " + MAG_TOKEN },
  };
  const response = await axios.get(
    `https://www.foodservicedirect.com/rest/V1/fsd/product-information/${id}/simple`,
    config
  );
  const product = response.data;

  return product;
};

const getBrandInfo = async (id) => {
  let mag_brand = {};
  try {
    const config = {
      headers: { Authorization: "bearer " + MAG_TOKEN },
    };
    const data = {
      sku: String(id),
      vendor_id: "5",
    };
    const response = await axios.post(
      `https://www.foodservicedirect.com/rest/V1/get-brand-info`,
      data,
      config
    );
    mag_brand = response.data;
  } catch (e) {
    return {};
  }

  return mag_brand;
};

const getConfigData = async (id) => {
  const config = {
    headers: { Authorization: "bearer " + MAG_TOKEN },
  };
  const response = await axios.get(
    `https://foodservicedirect.com/rest/V1/fsd/product-configurable/${id}`,
    config
  );
  const configData = response.data[0] || {};

  return configData;
};

const postCatalogRule = async (entity_id) => {
  const headers = {
    Accept: "*/*",
    "Content-Type": "application/json",
  };

  const bodyContent = {
    product_ids: [entity_id],
  };

  try {
    const response = await axios.post(
      "https://foodservicedirect.com/rest/V1/fsd/catalog_rule/price",
      bodyContent,
      { headers: headers }
    );

    return response.data;
  } catch (error) {
    console.error("Error:", error);
  }
};

const parseAndPopulateCSV = async (skuArray) => {
  if (!Array.isArray(skuArray) || skuArray.length === 0) {
    console.log("No SKUs provided.");
    return;
  }

  console.log(`Received ${skuArray.length} SKUs for processing...`);

  for (let start = 0; start < skuArray.length; start += 1200) {
    await processRows(skuArray, start, Math.min(start + 1200, skuArray.length));
  }

  console.log("first", DOC_REPO);
  console.log("All SKUs processed successfully.");
};

function logToFile(message) {
  const logDir = path.join(__dirname, "logs");
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir); // Create logs directory if not exists
  }

  const logFile = path.join(
    logDir,
    `${new Date().toISOString().split("T")[0]}.log`
  );
  const timestamp = new Date().toISOString();

  const logMessage = `[${timestamp}] ${message}\n`;

  fs.appendFileSync(logFile, logMessage, "utf8"); // Append log to file
}

const processRows = async (rows, start, end) => {
  const ids = [];
  let documents = [];
  let inactive_documents = [];

  for (let i = start; i < Math.min(end, rows.length); i++) {
    const sku = rows[i];

    let mag_product = {};
    let mag_product_fsd = {};
    let mag_product_info = {};
    let mag_brand_info = {};
    let configData = {};
    let gq_product = {};
    let mag_catalog_rule_price = [];
    try {
      mag_product = await getMagProduct(sku);
      mag_product_fsd = await getMagProductFSD(sku);
      mag_product_info = await getMagProducInfo(sku);
      mag_brand_info = await getBrandInfo(sku);

      if (mag_product_fsd.product_type === "configurable") {
        configData = await getConfigData(mag_product_fsd.sku);
        const optionSkus = configData?.swatch_information?.optionSkus;
        const foundKey = Object.entries(optionSkus).find(
          ([key, value]) => value === sku
        )?.[0];
        mag_catalog_rule_price = await postCatalogRule(foundKey);
      } else {
        mag_catalog_rule_price = await postCatalogRule(
          mag_product_fsd.entity_id
        );
      }

      gq_product = await getGQProduct(sku);
      console.log(JSON.stringify(gq_product));
    } catch (err) {
      console.log(err);
      inactive_documents.push({ id: sku, is_active: false });
      continue;
    }
    if (!mag_product.sku || !gq_product.sku) continue;
    // console.log(convertToDoc(urow, mag_product, mag_product_fsd, gq_product));
    // console.log(mag_product);
    // console.log(gq_product);
    // console.log(rows[i]);
    // console.log(mag_product_fsd);
    // process.exit();
    const doc = await convertToDoc(
      sku,
      mag_product,
      mag_product_fsd,
      mag_product_info,
      mag_brand_info,
      configData,
      gq_product,
      mag_catalog_rule_price
    );
    documents.push(doc);
    ids.push(sku);

    // if (i % 100 === 0) {
    //   try {
    //     const res = await typesense
    //       .collections(DOC_REPO)
    //       .documents()
    //       .import(documents, {
    //         action: "update",
    //         dirty_values: "coerce_or_drop",
    //       });
    //     // console.log(documents);
    //     console.log("typesense resp", res);
    //   } catch (err) {
    //     console.log("cxvxcvxcv", err);
    //   }
    //   if (inactive_documents.length > 0) {
    //     // @TEMP 7/6/2024
    //     try {
    //       const res_inactive = await typesense
    //         .collections(DOC_REPO)
    //         .documents()
    //         .import(inactive_documents, { action: "update" });
    //       console.log("MADE INACTIVES: " + inactive_documents.length);
    //       console.log(res_inactive);
    //     } catch (err) {
    //       console.log("cxvxcvxcv222", err);
    //     }
    //     // @TEMP 7/6/2024
    //   }
    //   documents = [];
    //   inactive_documents = [];
    // }
    // if (start === 160000)

    // }
  }

  try {
    const res = await typesense
      .collections(DOC_REPO)
      .documents()
      .import(documents, { action: "update", dirty_values: "coerce_or_drop" });
    console.log("typesense response", res);
    logToFile(
      `SUCCESS: Uploaded ${
        documents.length
      } documents to Typesense \n ${JSON.stringify(documents)}`
    );
  } catch (e) {
    logToFile(`ERROR: Failed to upload documents - ${e.message}`);
    console.log("typesense upload error", e);
  }
  if (inactive_documents.length > 0) {
    // @TEMP 7/6/2024
    try {
      const res_inactive = await typesense
        .collections(DOC_REPO)
        .documents()
        .import(inactive_documents, {
          action: "update",
          dirty_values: "coerce_or_drop",
        });
      console.log(res_inactive);
    } catch (e) {
      console.log(e);
    }
    // @TEMP 7/6/2024
  }
};

const cleanConfigAttributes = (data) => {
  if (data) {
    for (const attribute in data) {
      const options = data[attribute].options;

      // Clean each option
      options.forEach((option) => {
        // Replace false with empty string for vendorid and vendorname
        if (option.vendorid === false) option.vendorid = "";
        if (option.vendorname === false) option.vendorname = "";

        // Ensure products are arrays of strings
        if (!Array.isArray(option.products)) option.products = [];
      });
    }
  }
  return data;
};

const convertToDoc = async (
  sku,
  mag,
  magfsd,
  mag_info,
  mag_brand_info,
  configData,
  gq,
  mag_catalog_rule_price
) => {
  try {
    const gallery = [];
    mag.media_gallery_entries.map((g) =>
      gallery.push({
        id: g.id,
        original: `https://drryor7280ntb.cloudfront.net/media/catalog/product${g.file}`,
        thumbnail: `https://drryor7280ntb.cloudfront.net/media/catalog/product${g.file}`,
      })
    );
    const flags = [];
    let is_liquidation = false;
    let is_new_arrival = false;
    let is_rebate_eligible = false;
    let is_edlp = false;
    if (
      mag.custom_attributes.find((at) => at.attribute_code === "liquidation")
        ?.value == "1"
    ) {
      flags.push("liquidation");
      is_liquidation = true;
    }
    if (
      mag.custom_attributes.find((at) => at.attribute_code === "new_arrivals")
        ?.value == "1"
    ) {
      flags.push("new_arrivals");
      is_new_arrival = true;
    }
    if (
      mag.custom_attributes.find((at) => at.attribute_code === "product_tag")
        ?.value == "EDLP"
    ) {
      is_edlp = true;
    }
    if (String(gq.rebate_eligibility).toLocaleLowerCase() === "yes") {
      flags.push("rebate_eligible");
      is_rebate_eligible = true;
    }

    const manufacturer = mag.custom_attributes.find(
      (at) => at.attribute_code === "fsd_manufacturer"
    )?.value;
    const is_kitch =
      manufacturer?.toLocaleLowerCase().indexOf("kitch ") !== -1 ||
      String(gq.name).toLocaleLowerCase().indexOf("kitch 24/7") !== -1;
    const is_ufs = manufacturer?.toLocaleLowerCase().indexOf("unilever") !== -1;

    const current_price = getCurrentPrice({
      special_price: Number(gq.mp_special_price || mag.price),
      special_from_date: gq.mp_special_from_date || "",
      special_to_date: gq.mp_special_to_date || "",
      price: Number(mag.price),
    });

    const mag_attributes = [...mag.custom_attributes];
    for (var prop in gq || {}) {
      if (gq?.hasOwnProperty(prop)) {
        mag_attributes.push({ attribute_code: `gq_${prop}`, value: gq[prop] });
      }
    }
    // mag_attributes.push({ attribute_code: `is_returnable`, value: gq[prop] });
    // mag_attributes.push({ attribute_code: `shipping_temp`, value: gq[prop] });
    mag_attributes.push({
      attribute_code: `tier_prices`,
      value: magfsd.tier_prices,
    });
    // mag_attributes.push({ attribute_code: `bulk_each_sku`, value: gq[prop] });
    // mag_attributes.push({ attribute_code: `bulk_each_sku_url`, value: mag. });
    mag_attributes.push({
      attribute_code: `product_links`,
      value: mag.product_links,
    });
    mag_attributes.push({ attribute_code: `product_info`, value: mag_info });
    mag_attributes.push({
      attribute_code: `brand_info`,
      value: mag_brand_info,
    });

    const ship_temp_code = mag.custom_attributes.find(
      (at) => at.attribute_code === "shipping_temp"
    )?.value;
    const ship_temp =
      ship_temp_code == "130" ? "F" : ship_temp_code == "127" ? "R" : "D";
    const doc = {
      attributes: {
        dimension_unit: "IN",

        pricing_source: "RG",
        product_review: gq.product_review
          ? JSON.stringify(gq.product_review)
          : "",
        mag_status: mag.status,
        mag_visibility: mag.visibility,
        short_description:
          mag.custom_attributes.find(
            (at) => at.attribute_code === "short_description"
          )?.value || "",
        tax_class_id:
          mag.custom_attributes.find(
            (at) => at.attribute_code === "tax_class_id"
          )?.value || "0",
        upc10: mag.custom_attributes.find((at) => at.attribute_code === "upc10")
          ?.value,
        weight_unit: "LBS",
      },

      bulk_each: configData?.swatch_information?.bulk_each || null,
      catalog_rule_price: mag_catalog_rule_price,
      configAttributes:
        cleanConfigAttributes(configData?.swatch_information?.attributes) ||
        null,
      cost: 0,

      dimension_unit: "IN",
      entity_id: magfsd?.entity_id,
      flags: flags,
      gallery,
      gpo_collection: magfsd?.gpo_collection,

      haz_mat: "",

      id: sku,
      index: configData?.swatch_information?.index || null,
      is_active:
        gq.name != "" &&
        mag.custom_attributes.find(
          (at) => at.attribute_code === "fsd_product_reference"
        )?.value != "",
      is_in_stock: gq.is_in_stock,
      is_liquidation: is_liquidation,
      is_new_arrival: is_new_arrival,
      is_rebate_eligible: is_rebate_eligible,
      is_direct_deal: false, // @TODO - Need to add this attribute functionaly
      is_edlp: is_edlp,
      lang: "en-US",

      liquidation_expiry_date: gq.liquidation_expiry_date || "",
      mag_attributes,
      manufacturer: manufacturer,
      max_sale_qty:
        Number(mag?.extension_attributes?.stock_item?.max_sale_qty) || 2000,
      min_sale_qty:
        Number(mag?.extension_attributes?.stock_item?.min_sale_qty) || 1,

      name: gq.name,
      new_arrivals_expiry_date: gq.new_arrivals_expiry_date || "",
      options: magfsd?.options || null,
      optionSkus: configData?.swatch_information?.optionSkus || null,
      pack: 1,
      parent_sku: magfsd?.sku || null,
      platform_id: magfsd?.platform_id,
      price: Number(mag.price),

      product_id: mag.custom_attributes.find(
        (at) => at.attribute_code === "fsd_product_reference"
      )?.value,

      product_type: magfsd?.product_type,
      rating_avg: gq.product_review?.rating_avg || null,

      ships_in_days:
        gq.ships_in_days &&
        Number(gq.ships_in_days) > 0 &&
        gq.ships_in_days.indexOf("week") === -1
          ? Number(gq.ships_in_days)
          : Number(magfsd.shipsIn),

      shipping_type:
        String(magfsd?.shipping_type) || String(gq.shipping_type) || "",
      sku: sku,
      slug: String(gq.url).substring(Math.min(34, String(gq.url).length)),
      source: "pim",
      sort_price: current_price,
      special_price: current_price,
      special_from_date: gq.mp_special_from_date || "",
      special_to_date: gq.mp_special_to_date || "",
      status: gq.is_in_stock ? "In Stock" : "Out Of Stock",
      stock_type: gq.stock_type,
      title: gq.name,
      temperature: ship_temp,
      temperature_description:
        ship_temp == "D" ? "Dry" : ship_temp == "F" ? "Frozen" : "Refrigerated",

      use_config_min_sale_qty:
        Boolean(
          mag?.extension_attributes?.stock_item?.use_config_min_sale_qty
        ) || false,
      use_config_max_sale_qty:
        Boolean(
          mag?.extension_attributes?.stock_item?.use_config_max_sale_qty
        ) || false,
      variant: {
        parent_id: 10330478,
        children_ids: [12345, 10330478],
        variants: [
          {
            option_id: 1020,
            option_value: 16949,
            type: "pack_size",
            value: "6",
          },
          {
            option_id: 1076,
            option_value: 13449,
            type: "flavor",
            value: "chocolate",
          },
        ],
      },

      warehoused: true,

      weight_unit: "LBS",
    };

    return doc;
  } catch (er) {
    console.log("Error LOG", er);
  }
};

const getCurrentPrice = ({
  special_price,
  special_from_date,
  special_to_date,
  price,
}) => {
  let todayDate = new Date().toISOString().split("T")[0];
  const specialPrice = Number(special_price);
  const originalPrice = Number(price);

  if (specialPrice == null) {
    return originalPrice; // show price if special price null
  } else if (specialPrice && specialPrice >= originalPrice) {
    return originalPrice;
  } // show original price if special price is greater
  else if (
    specialPrice &&
    (!special_from_date || special_from_date === "") &&
    (!special_to_date || special_to_date === "")
  ) {
    return specialPrice; // always show special price
  } else if (
    specialPrice &&
    todayDate >= special_from_date &&
    todayDate <= special_to_date
  ) {
    return specialPrice; // special price show according to date
  } else if (
    specialPrice &&
    todayDate >= special_from_date &&
    special_to_date == null
  ) {
    return specialPrice; // special_to_date is null
  } else if (
    specialPrice &&
    special_from_date == null &&
    todayDate <= special_to_date
  ) {
    return specialPrice; // special_from_date is null
  } else {
    return originalPrice;
  }
};

// parseAndPopulateCSV();
// setTimeout(() => addAdditionalFlags(), 120 * 60 * 1000);

// API Endpoint to receive SKUs and process them
app.post("/process-skus", async (req, res) => {
  try {
    const { skus } = req.body; // Expecting { skus: ["sku1", "sku2", "sku3", ...] }

    if (!skus || !Array.isArray(skus) || skus.length === 0) {
      return res.status(400).json({ error: "Invalid or empty SKUs array" });
    }

    res
      .status(200)
      .json({ message: "SKUs processing started successfully.==>" + skus });
    // return;
    parseAndPopulateCSV(skus);

    // console.log(skus);
  } catch (error) {
    console.error("Error processing SKUs:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/process-skus-staging", async (req, res) => {
  try {
    DOC_REPO = "products_en-US_stage_v2";
    const { skus } = req.body; // Expecting { skus: ["sku1", "sku2", "sku3", ...] }

    if (!skus || !Array.isArray(skus) || skus.length === 0) {
      return res.status(400).json({ error: "Invalid or empty SKUs array" });
    }

    res
      .status(200)
      .json({ message: "SKUs processing started successfully.==>" + skus });
    // return;
    parseAndPopulateCSV(skus);

    // console.log(skus);
  } catch (error) {
    console.error("Error processing SKUs:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});
// Start the server
app.listen(PORT, () => {
  console.log(`Server is running on ${PORT}`);
});
