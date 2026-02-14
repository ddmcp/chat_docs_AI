document.addEventListener('DOMContentLoaded', () => {
    const chatContainer = document.getElementById('chat-container');
    const chatForm = document.getElementById('chat-form');
    const userInput = document.getElementById('user-input');
    const debugToggle = document.getElementById('debug-mode');

    // API Endpoint (Injected at runtime)
    const API_URL = '__CHAT_API_URL__';

    chatForm.addEventListener('submit', async (e) => {
        e.preventDefault();

        const message = userInput.value.trim();
        if (!message) return;

        // Add user message to UI
        addMessage(message, 'user');
        userInput.value = '';

        // Show typing indicator
        const typingId = showTypingIndicator();

        try {
            const response = await fetch(API_URL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    query: message,
                    limit: 3,
                    debug_flag: debugToggle.checked
                }),
            });

            if (!response.ok) {
                throw new Error(`API Error: ${response.statusText}`);
            }

            const data = await response.json();

            // Remove typing indicator
            removeTypingIndicator(typingId);

            // Add assistant response
            addMessage(data.final_llm_answer, 'assistant', data);

        } catch (error) {
            console.error('Error:', error);
            removeTypingIndicator(typingId);
            addMessage(`Sorry, I encountered an error: ${error.message}. Please make sure the backend is running at ${API_URL}`, 'assistant error');
        }
    });

    function addMessage(text, sender, data = null) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${sender}`;

        let contentHtml = `<div class="message-content">${marked.parse(text)}</div>`;

        // Add sources if available and in debug mode
        if (data && data.sources && debugToggle.checked) {
            let sourcesHtml = '<div class="sources"><strong>Sources (Click to expand):</strong>';
            data.sources.forEach((source, index) => {
                const sourceId = `source-${Date.now()}-${index}`;
                sourcesHtml += `
                    <div class="source-item" onclick="toggleSource('${sourceId}')">
                        <div class="source-header">
                            <span>${source.pdf_name} (p. ${source.page_number})</span>
                            <span class="score">Score: ${source.score.toFixed(3)}</span>
                        </div>
                        <div id="${sourceId}" class="source-details" style="display: none;">
                            <div class="source-info-grid">
                                <div class="info-item"><strong>Chunk ID:</strong> <span>${source.chunk_id}</span></div>
                                <div class="info-item"><strong>Chunk Index:</strong> <span>${source.chunk_index}</span></div>
                                <div class="info-item"><strong>PDF ID:</strong> <span>${source.pdf_id}</span></div>
                            </div>
                            <div class="source-text"><strong>Full Text:</strong><br>${source.full_text}</div>
                            <div class="source-meta"><strong>Additional Metadata:</strong><pre>${JSON.stringify(source.metadata, null, 2)}</pre></div>
                        </div>
                    </div>
                `;
            });
            sourcesHtml += '</div>';
            contentHtml += sourcesHtml;
        }

        // Add debug info if available
        if (data && data.debug_info && debugToggle.checked) {
            let debugHtml = '<div class="debug-info">';
            debugHtml += `<strong>🔍 Debug Information</strong><br>`;
            debugHtml += `<strong>Refined Query:</strong> ${data.debug_info.refined_query || 'N/A'}<br>`;
            debugHtml += `<strong>Duration:</strong> ${data.duration_seconds}s<br>`;
            debugHtml += `<strong>Total Documents Used:</strong> ${data.debug_info.total_documents || 0}<br>`;
            debugHtml += `<strong>Total Chunks Retrieved:</strong> ${data.debug_info.total_chunks || 0}<br><br>`;

            // Documents Used
            if (data.debug_info.documents_used && data.debug_info.documents_used.length > 0) {
                debugHtml += '<details><summary><strong>📄 Documents Used</strong></summary><div style="margin-left: 20px;">';
                data.debug_info.documents_used.forEach((doc, idx) => {
                    debugHtml += `<div style="margin: 10px 0; padding: 10px; background: rgba(255,255,255,0.05); border-radius: 5px;">`;
                    debugHtml += `<strong>${idx + 1}. ${doc.pdf_name}</strong><br>`;
                    debugHtml += `<small>PDF ID: ${doc.pdf_id}</small><br>`;
                    if (doc.metadata && Object.keys(doc.metadata).length > 0) {
                        debugHtml += `<details style="margin-top: 5px;"><summary>Metadata</summary><pre style="font-size: 11px;">${JSON.stringify(doc.metadata, null, 2)}</pre></details>`;
                    }
                    debugHtml += `</div>`;
                });
                debugHtml += '</div></details><br>';
            }

            // Chunks Details
            if (data.debug_info.chunks_details && data.debug_info.chunks_details.length > 0) {
                debugHtml += '<details><summary><strong>📦 Chunks Details</strong></summary><div style="margin-left: 20px;">';
                data.debug_info.chunks_details.forEach((chunk, idx) => {
                    debugHtml += `<div style="margin: 10px 0; padding: 10px; background: rgba(255,255,255,0.05); border-radius: 5px;">`;
                    debugHtml += `<strong>Chunk ${idx + 1}</strong><br>`;
                    debugHtml += `<strong>Similarity Score:</strong> ${chunk.similarity_score ? chunk.similarity_score.toFixed(4) : 'N/A'}<br>`;
                    debugHtml += `<strong>Source:</strong> ${chunk.pdf_name} (Page ${chunk.page_number}, Chunk ${chunk.chunk_index})<br>`;
                    debugHtml += `<small>Chunk ID: ${chunk.chunk_id}</small><br>`;
                    debugHtml += `<details style="margin-top: 5px;"><summary>Text Preview</summary><div style="font-size: 12px; margin-top: 5px;">${chunk.text_preview}</div></details>`;
                    if (chunk.metadata && Object.keys(chunk.metadata).length > 0) {
                        debugHtml += `<details style="margin-top: 5px;"><summary>Metadata</summary><pre style="font-size: 11px;">${JSON.stringify(chunk.metadata, null, 2)}</pre></details>`;
                    }
                    debugHtml += `</div>`;
                });
                debugHtml += '</div></details><br>';
            }

            // LLM Messages (Prompt Construction)
            if (data.debug_info.llm_messages && data.debug_info.llm_messages.length > 0) {
                debugHtml += '<details><summary><strong>💬 Prompt Messages</strong></summary><div style="margin-left: 20px;">';
                data.debug_info.llm_messages.forEach((msg, idx) => {
                    const type = msg.type || 'message';
                    const roleColor = type === 'system' ? '#818cf8' : '#38bdf8';
                    debugHtml += `<div style="margin: 10px 0; padding: 10px; background: rgba(255,255,255,0.05); border-radius: 5px; border-left: 3px solid ${roleColor};">`;
                    debugHtml += `<strong style="color: ${roleColor}; text-transform: uppercase; font-size: 10px;">${type}</strong><br>`;
                    debugHtml += `<div style="font-size: 12px; white-space: pre-wrap; margin-top: 5px;">${msg.content}</div>`;
                    debugHtml += `</div>`;
                });
                debugHtml += '</div></details>';
            }

            debugHtml += '</div>';
            contentHtml += debugHtml;
        }

        messageDiv.innerHTML = contentHtml;
        chatContainer.appendChild(messageDiv);

        // Scroll to bottom
        chatContainer.scrollTop = chatContainer.scrollHeight;
    }

    function showTypingIndicator() {
        const id = 'typing-' + Date.now();
        const typingDiv = document.createElement('div');
        typingDiv.className = 'message assistant typing';
        typingDiv.id = id;
        typingDiv.innerHTML = `
            <div class="typing-indicator">
                <span></span>
                <span></span>
                <span></span>
            </div>
        `;
        chatContainer.appendChild(typingDiv);
        chatContainer.scrollTop = chatContainer.scrollHeight;
        return id;
    }

    function removeTypingIndicator(id) {
        const el = document.getElementById(id);
        if (el) el.remove();
    }

    // Global toggle function
    window.toggleSource = (id) => {
        const el = document.getElementById(id);
        if (el) {
            el.style.display = el.style.display === 'none' ? 'block' : 'none';
        }
    };
});
