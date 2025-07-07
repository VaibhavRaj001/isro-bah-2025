import express from 'express';
import cors from 'cors';
import 'dotenv/config';
import { Pinecone } from '@pinecone-database/pinecone';
import { PineconeStore } from '@langchain/pinecone';
import { OpenAIEmbeddings, ChatOpenAI } from '@langchain/openai';

const app = express();
app.use(cors());
app.use(express.json());

const pinecone = new Pinecone({ apiKey: process.env.PINECONE_API_KEY });
const index = pinecone.Index(process.env.PINECONE_INDEX);

const embeddings = new OpenAIEmbeddings({
    modelName: 'text-embedding-3-small',
    openAIApiKey: process.env.OPENAI_API_KEY,
});

let retrieverStatic, retrieverPdf;

const initPinecone = async () => {
    retrieverStatic = await PineconeStore.fromExistingIndex(embeddings, {
        pineconeIndex: index,
        namespace: 'mosdac',
    });

    retrieverPdf = await PineconeStore.fromExistingIndex(embeddings, {
        pineconeIndex: index,
        namespace: 'mosdac-pdf',
    });
};

app.get('/', (req, res) => {
    res.json("Everything working fine!!");
});

app.post('/chat', async (req, res) => {
    const userQuery = req.body.message;

    if (!userQuery || typeof userQuery !== 'string' || userQuery.trim() === '') {
        return res.status(400).json({ message: "Missing or empty 'message' in request body." });
    }

    try {
        const model = new ChatOpenAI({
            modelName: 'gpt-3.5-turbo',
            temperature: 0,
            openAIApiKey: process.env.OPENAI_API_KEY,
        });

        const [resultsStatic, resultsPdf] = await Promise.all([
            retrieverStatic.similaritySearch(userQuery, 3),
            retrieverPdf.similaritySearch(userQuery, 3),
        ]);

        const allDocs = [...resultsStatic, ...resultsPdf];
        const combinedContext = allDocs.map(doc => doc.pageContent).join('\n\n');

        const response = await model.call([
            { role: 'system', content: 'You are a helpful assistant for MOSDAC-related queries.' },
            { role: 'user', content: `Answer the following based on:\n${combinedContext}\n\nQuestion: ${userQuery}` },
        ]);

        res.json({
            messages: [
                { role: 'user', content: userQuery },
                { role: 'system', content: response.content },
            ],
        });
    } catch (err) {
        console.error('Chat error:', err);
        res.status(500).json({ message: 'Internal error' });
    }
});


app.get('/team', (req, res) => {
    res.json("This is the teams page");
});

initPinecone().then(() => {
    app.listen(3000, () => {
        console.log('Server started on http://localhost:3000');
    });
});
