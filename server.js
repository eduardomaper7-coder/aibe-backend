import express from "express";
import dotenv from "dotenv";
import { extractRestaurantData } from "./blogGenerator/services/restaurantExtractor.js";
import { generateArticles } from "./blogGenerator/services/articleGenerator.js";

dotenv.config();

const app = express();
app.use(express.json());

app.post("/generate", async (req, res) => {
  try {
    const { googleMapsUrl } = req.body;
    if (!googleMapsUrl) {
      return res.status(400).json({ error: "Missing googleMapsUrl" });
    }

    const restaurantData = await extractRestaurantData(googleMapsUrl);
    const articles = await generateArticles(
      restaurantData,
      googleMapsUrl
    );

    res.json({
      restaurant: restaurantData,
      articles
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Generation failed" });
  }
});

app.listen(process.env.PORT, () => {
  console.log(`Server running on port ${process.env.PORT}`);
});
