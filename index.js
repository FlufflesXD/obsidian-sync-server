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

// Convert room name back to file path (reverse of pathToRoom)
function roomToPath(room) {
    // Reverse URL-safe base64
    let base64 = room.replace(/-/g, '+').replace(/_/g, '/');
    // Add padding if needed
    while (base64.length % 4 !== 0) {
        base64 += '=';
    }
    return decodeURIComponent(Buffer.from(base64, 'base64').toString('utf-8'));
}

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

        // Note: We no longer delete "empty" docs automatically.
        // Empty files are valid - the deletion logic was causing race conditions
        // where new files got deleted before content could sync.

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

        // For file index, validate that all entries exist in MinIO
        if (docName === '__file_index__') {
            await validateFileIndex(ydoc);
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

// Validate file index against MinIO - remove entries for non-existent files
async function validateFileIndex(ydoc) {
    const fileIndex = ydoc.getMap('files');
    const allKeys = Array.from(fileIndex.keys());

    console.log('[FILE INDEX VALIDATION] Checking', allKeys.length, 'entries against MinIO...');

    const keysToDelete = [];

    for (const key of allKeys) {
        // Skip folders, only check files
        const metadata = fileIndex.get(key);
        if (metadata?.type === 'folder') continue;

        // Check if the Yjs file exists in MinIO
        const minioKey = `yjs/${key.replace(/[^a-zA-Z0-9._-]/g, '_')}.yjs`;
        try {
            await minioClient.statObject(BUCKET_NAME, minioKey);
            // File exists, keep the entry
        } catch (err) {
            if (err.code === 'NotFound') {
                console.log(`[FILE INDEX VALIDATION] Removing orphan: ${key} (not in MinIO)`);
                keysToDelete.push(key);
            }
        }
    }

    // Delete orphaned entries
    if (keysToDelete.length > 0) {
        ydoc.transact(() => {
            for (const key of keysToDelete) {
                fileIndex.delete(key);
            }
        });
        console.log(`[FILE INDEX VALIDATION] Removed ${keysToDelete.length} orphaned entries`);
    }
}

// --- HTTP + WebSocket Server ---
const server = http.createServer(async (req, res) => {
    // CORS headers for cross-origin requests
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
        res.writeHead(200);
        res.end();
        return;
    }

    const url = new URL(req.url, `http://${req.headers.host}`);

    // GET /files - List all synced files from MinIO
    if (url.pathname === '/files' && req.method === 'GET') {
        try {
            const files = [];
            const objectStream = minioClient.listObjects(BUCKET_NAME, 'yjs/', true);

            for await (const obj of objectStream) {
                // Extract room name from MinIO key: yjs/roomname.yjs -> roomname
                const key = obj.name;
                if (key.startsWith('yjs/') && key.endsWith('.yjs')) {
                    const roomName = key.slice(4, -4); // Remove 'yjs/' prefix and '.yjs' suffix

                    // Skip special rooms
                    if (roomName === '__file_index__') continue;

                    // Decode room name back to file path (reverse of pathToRoom)
                    try {
                        const filePath = roomToPath(roomName);
                        files.push(filePath);
                    } catch (err) {
                        // If decoding fails, it might be old format - try legacy conversion
                        console.log(`[/files] Failed to decode room: ${roomName}, using legacy`);
                        // Fallback for old format (if any) - this part might need adjustment
                        // based on how old room names were generated.
                        // For now, we'll just log and skip if it's not base64url.
                        // If old format was just replacing '/' with '_', you could add:
                        // files.push(roomName.replace(/_/g, '/'));
                    }
                }
            }

            console.log(`[/files] Returning ${files.length} files`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ files }));
        } catch (err) {
            console.error('[/files] Error:', err.message);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: err.message }));
        }
        return;
    }

    // Default response
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
