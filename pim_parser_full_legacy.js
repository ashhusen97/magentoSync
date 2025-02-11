const Typesense = require("typesense");
const axios = require("axios");
const express = require("express");
const bodyParser = require("body-parser");
const app = express();
const PORT = 3000;
// Middleware to parse JSON request body
app.use(bodyParser.json());
const DOC_REPO = "products_en-US_v8";
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
    Authorization: "bearer " + MAG_TOKEN,
  };
  const graphqlQuery = {
    query: `{
      products :product_details(skus_comma_separated:"${id}"){
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

const parseAndPopulateCSV = async (skuArray) => {
  if (!Array.isArray(skuArray) || skuArray.length === 0) {
    console.log("No SKUs provided.");
    return;
  }

  console.log(`Received ${skuArray.length} SKUs for processing...`);

  for (let start = 0; start < skuArray.length; start += 1200) {
    await processRows(skuArray, start, Math.min(start + 1200, skuArray.length));
  }

  console.log("All SKUs processed successfully.");
};
let counter = 0;
const count = () => counter++;

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
    try {
      mag_product = await getMagProduct(sku);
      mag_product_fsd = await getMagProductFSD(sku);
      mag_product_info = await getMagProducInfo(sku);
      mag_brand_info = await getBrandInfo(sku);
      if (mag_product_fsd.product_type === "configurable") {
        configData = await getConfigData(mag_product_fsd.sku);
      }

      gq_product = await getGQProduct(sku);
    } catch (err) {
      console.log(err);
      // try {
      //   await typesense
      //     .collections(DOC_REPO)
      //     .documents(urow.sku)
      //     .delete();
      // } catch (del_err) {}
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
      gq_product
    );
    documents.push(doc);
    ids.push(sku);
    console.log(documents);
    if (i % 100 === 0) {
      try {
        const res = await typesense
          .collections(DOC_REPO)
          .documents()
          .import(documents, {
            action: "update",
            dirty_values: "coerce_or_drop",
          });
        // console.log(documents);
        console.log(res);
      } catch (err) {
        console.log(err);
      }
      if (inactive_documents.length > 0) {
        // @TEMP 7/6/2024
        try {
          const res_inactive = await typesense
            .collections(DOC_REPO)
            .documents()
            .import(inactive_documents, { action: "update" });
          console.log("MADE INACTIVES: " + inactive_documents.length);
          console.log(res_inactive);
        } catch (err) {
          console.log(err);
        }
        // @TEMP 7/6/2024
      }
      documents = [];
      inactive_documents = [];
    }
    // if (start === 160000)
    let c = count();
    printProgress(
      `inserting item ${c} of ${rows.length} ${(
        (c / rows.length) *
        100
      ).toFixed(3)}%`
    );
    // }
  }
  try {
    const res = await typesense
      .collections(DOC_REPO)
      .documents()
      .import(documents, { action: "update", dirty_values: "coerce_or_drop" });
    console.log(res);
  } catch (e) {
    console.log(e);
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
  gq
) => {
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
  mag_attributes.push({ attribute_code: `brand_info`, value: mag_brand_info });

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
        mag.custom_attributes.find((at) => at.attribute_code === "tax_class_id")
          ?.value || "0",
      upc10: mag.custom_attributes.find((at) => at.attribute_code === "upc10")
        ?.value,
      weight_unit: "LBS",
    },

    bulk_each: configData?.swatch_information?.bulk_each || null,

    configAttributes:
      cleanConfigAttributes(configData?.swatch_information?.attributes) || null,
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
    special_price: gq.mp_special_price || Number(magfsd.price),
    special_from_date: gq.mp_special_from_date || "",
    special_to_date: gq.mp_special_to_date || "",
    status: gq.is_in_stock ? "In Stock" : "Out Of Stock",
    stock_type: gq.stock_type,
    title: gq.name,
    temperature: ship_temp,
    temperature_description:
      ship_temp == "D" ? "Dry" : ship_temp == "F" ? "Frozen" : "Refrigerated",

    use_config_min_sale_qty:
      Boolean(mag?.extension_attributes?.stock_item?.use_config_min_sale_qty) ||
      false,
    use_config_max_sale_qty:
      Boolean(mag?.extension_attributes?.stock_item?.use_config_max_sale_qty) ||
      false,
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
};

const addAdditionalFlags = async () => {
  const direct_deal_skus = [
    "2976902",
    "9332",
    "190272",
    "23048093",
    "161913",
    "161942",
    "1219593",
    "21201825",
    "22993680",
    "2976960",
  ];

  const featured_skus = [
    "168232",
    "2964764",
    "2964594",
    "2944584",
    "209282",
    "315409",
    "315473",
    "315545",
    "315719",
    "315755",
    "315475",
    "315476",
    "315477",
    "315477",
    "315480",
    "21405984",
    "315337",
    "315400",
    "315457",
    "315625",
    "315143",
    "315481",
    "315483",
    "21405988",
    "21405992",
    "314342",
    "21263376",
    "2970828",
    "2970827",
    "2970826",
    "211059",
  ];

  const trending_skus = [
    "21263681",
    "21261624",
    "21261623",
    "209322",
    "172723",
    "315127",
    "315128",
    "315129",
    "315130",
    "315131",
    "314294",
    "314367",
    "314369",
    "314370",
    "314372",
    "315144",
    "315145",
    "315146",
    "315147",
    "315148",
    "314343",
    "314702",
    "314703",
    "314737",
    "314749",
    "211058",
    "211057",
    "211056",
    "2943806",
    "169044",
  ];

  const docs = [];

  for (let w = 0; w < direct_deal_skus.length; w++) {
    const doc = await typesense
      .collections(DOC_REPO)
      .documents(direct_deal_skus[w])
      .retrieve();
    docs.push({
      id: doc.id,
      flags:
        doc.flags.indexOf("direct_deal") === -1
          ? [...doc.flags, "direct_deal"]
          : [...doc.flags],
      is_direct_deal: true,
    });
  }
  for (let w = 0; w < featured_skus.length; w++) {
    const doc = await typesense
      .collections(DOC_REPO)
      .documents(featured_skus[w])
      .retrieve();
    docs.push({
      id: doc.id,
      flags:
        doc.flags.indexOf("featured") === -1
          ? [...doc.flags, "featured"]
          : [...doc.flags],
    });
  }
  for (let w = 0; w < trending_skus.length; w++) {
    const doc = await typesense
      .collections(DOC_REPO)
      .documents(trending_skus[w])
      .retrieve();
    docs.push({
      id: doc.id,
      flags:
        doc.flags.indexOf("trending") === -1
          ? [...doc.flags, "trending"]
          : [...doc.flags],
    });
  }
  console.log(docs);
  try {
    await typesense
      .collections(DOC_REPO)
      .documents()
      .import(docs, { action: "update" });
  } catch (errrr) {
    // continue;
  }
};

const printProgress = (progress) => {
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(progress);
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

// Start the server
app.listen(PORT, () => {
  console.log(`Server is running on ${PORT}`);
});
