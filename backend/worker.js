import fs from 'fs';
import readline from 'readline';
import { Pinecone } from '@pinecone-database/pinecone';
import { OpenAIEmbeddings } from "@langchain/openai";
import { PineconeStore } from "@langchain/pinecone";
import { RecursiveCharacterTextSplitter } from "langchain/text_splitter";
import 'dotenv/config'

//console.log(process.env.PINECONE_API_KEY)
//this fill adds jsonl files to pinecone

const pinecone = new Pinecone({
  apiKey: process.env.PINECONE_API_KEY,
});
const index = pinecone.Index(process.env.PINECONE_INDEX);

const embeddings = new OpenAIEmbeddings({
  openAIApiKey: process.env.OPENAI_API_KEY,
  model: "text-embedding-3-small"
});

const processJsonlFile = async (filePath, namespace) => {
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line.trim()) continue;

    const doc = JSON.parse(line);
    const text = doc.text;
    const metadata = {
      id: doc.id,
      url: doc.url,
      title: doc.title,
    };

    const splitter = new RecursiveCharacterTextSplitter({
      chunkSize: 1000,
      chunkOverlap: 200,
    });

    const chunks = await splitter.createDocuments([text], [metadata]);

    await PineconeStore.fromDocuments(chunks, embeddings, {
      pineconeIndex: index,
      namespace: namespace,
        });

    console.log(`Embedded and stored: ${doc.id}`);
  }
};

processJsonlFile('./mosdac_pdfs_text.jsonl', 'mosdac-pdf')
  .then(() => console.log('All documents processed and embedded.'))
  .catch(console.error);