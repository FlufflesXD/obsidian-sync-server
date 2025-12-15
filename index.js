#!/usr/bin/env node

// Using CommonJS for y-websocket compatibility
const WebSocket = require('ws');
const http = require('http');
const Minio = require('minio');
const Y = require('yjs');
const { setupWSConnection, setPersistence, docs } = require('y-websocket/bin/utils');

// Configuration from environment variables
const PORT = process.env.PORT || 8080;

// --- MinIO Configuration ---
const minioClient = new Minio.Client({
    endPoint: process.env.MINIO_ENDPOINT || 'minio.maksimvenne.info',
    port: parseInt(process.env.MINIO_PORT || '9000'),
    useSSL: process.env.MINIO_USE_SSL === 'true',
    accessKey: process.env.MINIO_ACCESS_KEY || 'admin',
    secretKey: process.env.MINIO_SECRET_KEY || 'I8d0NsDGvwxuev'
});

const BUCKET_NAME = process.env.MINIO_BUCKET || 'obsidian-sync';

// Ensure bucket exists
async function ensureBucket() {
    try {
        const exists = await minioClient.bucketExists(BUCKET_NAME);
        if (!exists) {
            await minioClient.makeBucket(BUCKET_NAME, 'us-east-1');
            console.log(`Bucket '${BUCKET_NAME}' created.`);
        } else {
            console.log(`Bucket '${BUCKET_NAME}' exists.`);
        }
    } catch (err) {
        console.error('Error ensuring bucket:', err);
    }
}

// Load Yjs doc from MinIO
async function loadFromMinIO(docName) {
    try {
        const stream = await minioClient.getObject(BUCKET_NAME, `yjs/${docName}.yjs`);
        const chunks = [];
        for await (const chunk of stream) {
            chunks.push(chunk);
        }
        return Buffer.concat(chunks);
    } catch (err) {
        if (err.code !== 'NoSuchKey') {
            console.error(`Error loading ${docName} from MinIO:`, err.message);
        }
        return null;
    }
}

// Save Yjs doc to MinIO
const saveLocks = new Map(); // Prevent concurrent saves

async function saveToMinIO(docName, ydoc) {
    // Prevent concurrent saves for same doc
    if (saveLocks.get(docName)) {
        return; // Skip if already saving
    }
    saveLocks.set(docName, true);

    try {
        const state = Y.encodeStateAsUpdate(ydoc);
        const buffer = Buffer.from(state);

        // Check if doc is essentially empty (deleted)
        const text = ydoc.getText('content').toString();
        if (text.length === 0 && docName !== '__file_index__') {
            // Doc is empty, delete from MinIO instead of saving
            try {
                await minioClient.removeObject(BUCKET_NAME, `yjs/${docName}.yjs`);
                console.log(`Deleted ${docName} from MinIO (empty doc)`);
            } catch (err) {
                if (err.code !== 'NoSuchKey') {
                    console.error(`Error deleting ${docName}:`, err.message);
                }
            }
            return;
        }

        await minioClient.putObject(BUCKET_NAME, `yjs/${docName}.yjs`, buffer, buffer.length);
        console.log(`Saved ${docName} to MinIO`);
    } catch (err) {
        console.error(`Error saving ${docName} to MinIO:`, err.message);
    } finally {
        saveLocks.delete(docName);
    }
}

// Debounce saves
const saveTimers = new Map();
const SAVE_DEBOUNCE_MS = 3000; // Increased to 3 seconds

function debouncedSave(docName, ydoc) {
    if (saveTimers.has(docName)) {
        clearTimeout(saveTimers.get(docName));
    }
    saveTimers.set(docName, setTimeout(() => {
        saveToMinIO(docName, ydoc);
        saveTimers.delete(docName);
    }, SAVE_DEBOUNCE_MS));
}

// Set up MinIO persistence for y-websocket
setPersistence({
    provider: null,
    bindState: async (docName, ydoc) => {
        console.log(`Binding state for: ${docName}`);
        const data = await loadFromMinIO(docName);
        if (data) {
            Y.applyUpdate(ydoc, new Uint8Array(data));
            console.log(`Loaded ${docName} from MinIO`);
        }
        // Save on updates
        ydoc.on('update', () => {
            debouncedSave(docName, ydoc);
        });
    },
    writeState: async (docName, ydoc) => {
        await saveToMinIO(docName, ydoc);
    }
});

// --- HTTP + WebSocket Server ---
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Yjs sync server running');
});

const wss = new WebSocket.Server({ noServer: true });

wss.on('connection', (ws, req) => {
    setupWSConnection(ws, req);
    console.log('Client connected');
});

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

// Start server
ensureBucket().then(() => {
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`Yjs WebSocket server running on port ${PORT}`);
    });
});
