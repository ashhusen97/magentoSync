const express = require("express");
const bodyParser = require("body-parser");
const Typesense = require("typesense");

// Create Express app
const app = express();
const port = process.env.PORT || 3000; // Use the environment variable PORT if available, otherwise default to 3000

app.use(bodyParser.json());

// Initialize Typesense client
let typesenseClient = new Typesense.Client({
  nodes: [
    {
      host: "integration-typesense.foodservicedirect.ca",
      port: 443,
      path: "",
      protocol: "https",
    },
  ],
  apiKey: "vFWSoLHDWdbfnxVMu0D8Cf3d8LEqGetjFLr9fMT8Od2066NY",
  connectionTimeoutSeconds: 100,
});
const updateMagAttributesSimple = (body) => {
  let updatedMagAttributes = [];

  // Check for each field and update mag_attributes accordingly
  if (body.new_arrivals) {
    updatedMagAttributes.push({
      attribute_code: "new_arrivals",
      value: body.new_arrivals,
    });
  }

  if (body.description) {
    updatedMagAttributes.push({
      attribute_code: "description",
      value: body.description,
    });
  }

  if (body.meta_title) {
    updatedMagAttributes.push({
      attribute_code: "meta_title",
      value: body.meta_title,
    });
  }

  if (body.liquidation) {
    updatedMagAttributes.push({
      attribute_code: "liquidation",
      value: body.liquidation,
    });
  }

  return updatedMagAttributes;
};

// Webhook endpoint to receive product updates from Magento
app.post("/webhook/magento-product-update", async (req, res) => {
  try {
    console.log(req.body);
    const productData = req.body; // Assuming Magento sends product data in the webhook payload

    // Update or upsert product in Typesense
    // const typesenseResponse = await typesenseClient.collections('staging_CA_v1').documents().upsert();
    const updatedMagAttributes = updateMagAttributesSimple(productData);
    const typesenseResponse = await typesenseClient
      .collections("staging_CA_v2")
      .documents()
      .import(
        {
          id: "" + productData.id,
          price: productData.price,
          pack: productData.pack,
          new_arrivals_expiry_date: productData.new_arrivals_expiry_date,
          name: productData.name,
          mpn: productData.mpn,
          shipping_type: productData.shipping_type,
          ships_in_days: productData.ships_in_days,
          special_from_date: productData.special_from_date,
          special_to_date: productData.special_to_date,
          status: productData.status,
          taxable: productData.taxable,
          temperature: productData.temperature,
          temperature_description: productData.temperature_description,
          title: productData.title,
          upc: productData.upc,
          is_active: productData.is_active,
          is_in_stock: productData.is_in_stock,
          category_l1: productData.category_l1,
          category_l2: productData.category_l2,
          category_l3: productData.category_l3,
          category_l4: productData.category_l4,
          case_quantity: productData.case_quantity,
          brand: productData.brand,
          new_arrivals_expiry_date: productData.new_arrivals_expiry_date,
          mag_attributes: updatedMagAttributes,
        },
        { action: "update" }
      );

    console.log("Product synced with Typesense:", typesenseResponse);

    // Respond to Magento that the webhook was received and processed
    res.status(200).send("Webhook received and processed");
  } catch (error) {
    console.error("Error processing webhook:", error);
    res.status(500).send("Error processing webhook", error);
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Webhook listener running on port ${port}`);
});
