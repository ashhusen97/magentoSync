const express = require('express');
const bodyParser = require('body-parser');
const Typesense = require('typesense');

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

// Webhook endpoint to receive product updates from Magento
app.post('/webhook/magento-product-update', async (req, res) => {
  try {
    console.log(req.body)
    const productData = req.body;  // Assuming Magento sends product data in the webhook payload

    // Update or upsert product in Typesense
    // const typesenseResponse = await typesenseClient.collections('staging_CA_v1').documents().upsert();

    const typesenseResponse =   await typesenseClient
    .collections("staging_CA_v1")
    .documents()
    .import({
        id: productData.id,  // Assuming Magento sends the product ID
        price: productData.price,
     
      }, { action: "update" });

    console.log('Product synced with Typesense:', typesenseResponse);

    // Respond to Magento that the webhook was received and processed
    res.status(200).send('Webhook received and processed');
  } catch (error) {
    console.error('Error processing webhook:', error);
    res.status(500).send('Error processing webhook',error);
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Webhook listener running on port ${port}`);
});
