package com.remodex.android.service

import android.util.Base64
import android.util.Log
import com.remodex.android.data.model.*
import com.remodex.android.data.store.MessagePersistence
import com.remodex.android.data.store.SecureStore
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import java.net.URLEncoder
import java.security.MessageDigest

class CodexService(
    private val secureStore: SecureStore,
    private val messagePersistence: MessagePersistence,
    private val connectionManager: ConnectionManager,
    private val secureTransport: SecureTransport,
    private val messageTransport: MessageTransport,
    private val historyDecoder: HistoryDecoder,
    private val okHttpClient: OkHttpClient,
    private val runCompletionNotifier: RunCompletionNotifier,
    private val backgroundTurnMonitor: BackgroundTurnMonitor,
    private val json: Json
) {
    companion object {
        private const val TAG = "CodexService"
        private const val APP_VERSION = "1.1.2"
        private const val THREAD_LIST_LIMIT = 50
        private const val HANDSHAKE_MODE_TRUSTED_RECONNECT = "trusted_reconnect"
    }

    private data class RelayConnectionTarget(
        val relayUrl: String,
        val sessionId: String,
        val nextHandshakeMode: String
    )

    private val scope = CoroutineScope(Dispatchers.Main + SupervisorJob())
    private val _threads = MutableStateFlow<List<CodexThread>>(emptyList())
    val threads: StateFlow<List<CodexThread>> = _threads.asStateFlow()

    private val connectionPresentationCoordinator = ConnectionPresentationCoordinator(
        scope = scope,
        isConnecting = connectionManager.isConnecting,
        isConnected = connectionManager.isConnected,
        secureState = secureTransport.state,
        threadCount = _threads.map { it.size }
    )
    private val threadHistoryHydrator = ThreadHistoryHydrator(
        scope = scope,
        isThreadArchived = { threadId ->
            thread(threadId)?.syncState == CodexThreadSyncState.ARCHIVED_LOCAL
                || locallyArchivedThreadIDs().contains(threadId)
        },
        syncThreadHistory = ::syncThreadHistory,
        shouldTreatErrorAsHydrated = ::shouldMarkThreadHistoryHydratedAfterError,
        logTag = TAG
    )
    private val assistantHandler = AssistantHandler()
    private val incomingRouter = IncomingRouter(
        secureTransport = secureTransport,
        messageTransport = messageTransport,
        json = json,
        onNotification = ::handleNotification,
        onRequest = ::handleServerRequest
    ).apply {
        sendRawCallback = { text -> connectionManager.send(text) }
    }

    // --- State ---
    private val _activeThreadId = MutableStateFlow<String?>(null)
    val activeThreadId: StateFlow<String?> = _activeThreadId.asStateFlow()

    private val postConnectSyncCoordinator = PostConnectSyncCoordinator(
        refreshAccountStatus = ::refreshAccountStatusNow,
        refreshModels = ::refreshModelsNow,
        syncThreadList = ::syncThreadList,
        refreshTrackedRunningTurnStates = ::refreshTrackedRunningTurnStates,
        routePendingNotificationOpenIfPossible = ::routePendingNotificationOpenIfPossible,
        activeThreadId = { _activeThreadId.value },
        hydrateThreadHistory = { threadId ->
            loadThreadHistoryIfNeeded(threadId, forceRefresh = true)
        },
        onBootstrapStateChanged = connectionPresentationCoordinator::setBootstrapping,
        onBootstrapCompleted = {
            connectionPresentationCoordinator.markRecoveryIdle()
            startSyncLoops()
        }
    )

    private val _messagesByThread = MutableStateFlow<Map<String, List<CodexMessage>>>(emptyMap())
    val messagesByThread: StateFlow<Map<String, List<CodexMessage>>> = _messagesByThread.asStateFlow()

    private val _runningThreadIDs = MutableStateFlow<Set<String>>(emptySet())
    val runningThreadIDs: StateFlow<Set<String>> = _runningThreadIDs.asStateFlow()

    private val _readyThreadIDs = MutableStateFlow<Set<String>>(emptySet())
    val readyThreadIDs: StateFlow<Set<String>> = _readyThreadIDs.asStateFlow()

    private val _failedThreadIDs = MutableStateFlow<Set<String>>(emptySet())
    val failedThreadIDs: StateFlow<Set<String>> = _failedThreadIDs.asStateFlow()

    val connectionRecoveryState: StateFlow<CodexConnectionRecoveryState> =
        connectionPresentationCoordinator.connectionRecoveryState
    val isBootstrappingConnectionSync: StateFlow<Boolean> =
        connectionPresentationCoordinator.isBootstrappingConnectionSync
    val isLoadingThreads: StateFlow<Boolean> = connectionPresentationCoordinator.isLoadingThreads
    val isLoadingModels: StateFlow<Boolean> = connectionPresentationCoordinator.isLoadingModels
    val loadingThreadIds: StateFlow<Set<String>> = threadHistoryHydrator.loadingThreadIds
    val connectionPhase: StateFlow<CodexConnectionPhase> =
        connectionPresentationCoordinator.connectionPhase

    private val _pendingApproval = MutableStateFlow<ApprovalRequest?>(null)
    val pendingApproval: StateFlow<ApprovalRequest?> = _pendingApproval.asStateFlow()

    private val _availableModels = MutableStateFlow<List<CodexModelOption>>(emptyList())
    val availableModels: StateFlow<List<CodexModelOption>> = _availableModels.asStateFlow()

    private val _selectedModel = MutableStateFlow<String?>(null)
    val selectedModel: StateFlow<String?> = _selectedModel.asStateFlow()

    private val _selectedReasoningEffort = MutableStateFlow<String?>(null)
    val selectedReasoningEffort: StateFlow<String?> = _selectedReasoningEffort.asStateFlow()

    private val _selectedServiceTier = MutableStateFlow<CodexServiceTier?>(null)
    val selectedServiceTier: StateFlow<CodexServiceTier?> = _selectedServiceTier.asStateFlow()

    private val _selectedAccessMode = MutableStateFlow(CodexAccessMode.ON_REQUEST)
    val selectedAccessMode: StateFlow<CodexAccessMode> = _selectedAccessMode.asStateFlow()

    private val _contextWindowUsage = MutableStateFlow<ContextWindowUsage?>(null)
    val contextWindowUsage: StateFlow<ContextWindowUsage?> = _contextWindowUsage.asStateFlow()

    private val _rateLimitBuckets = MutableStateFlow<List<CodexRateLimitBucket>>(emptyList())
    val rateLimitBuckets: StateFlow<List<CodexRateLimitBucket>> = _rateLimitBuckets.asStateFlow()

    private val _bridgeVersionInfo = MutableStateFlow<String?>(null)
    val bridgeVersionInfo: StateFlow<String?> = _bridgeVersionInfo.asStateFlow()

    private val _gptAccountSnapshot = MutableStateFlow(CodexGPTAccountSnapshot())
    val gptAccountSnapshot: StateFlow<CodexGPTAccountSnapshot> = _gptAccountSnapshot.asStateFlow()

    private val _gptAccountErrorMessage = MutableStateFlow<String?>(null)
    val gptAccountErrorMessage: StateFlow<String?> = _gptAccountErrorMessage.asStateFlow()

    private val _aiChangeSetRevision = MutableStateFlow(0)
    val aiChangeSetRevision: StateFlow<Int> = _aiChangeSetRevision.asStateFlow()

    private val _missingNotificationThreadPrompt =
        MutableStateFlow<CodexMissingNotificationThreadPrompt?>(null)
    val missingNotificationThreadPrompt: StateFlow<CodexMissingNotificationThreadPrompt?> =
        _missingNotificationThreadPrompt.asStateFlow()

    val isConnected: StateFlow<Boolean> = connectionManager.isConnected
    val isConnecting: StateFlow<Boolean> = connectionManager.isConnecting
    val secureConnectionState: StateFlow<CodexSecureConnectionState> = secureTransport.state

    private val activeTurnIdByThread = mutableMapOf<String, String>()
    private val aiChangeSetsById = mutableMapOf<String, AIChangeSet>()
    private val aiChangeSetIdByAssistantMessageId = mutableMapOf<String, String>()
    private val aiChangeSetIdByTurnId = mutableMapOf<String, String>()
    private val repoRootByWorkingDirectory = mutableMapOf<String, String>()
    private val mirroredRunningCatchupThreadIDs = mutableSetOf<String>()
    private var pendingGPTLoginId: String? = null
    private var syncJob: Job? = null
    private var currentConnectionTarget: RelayConnectionTarget? = null
    private var supportsStructuredSkillInput = true
    private var supportsTurnCollaborationMode = false
    private var isAppInForeground = true

    private val _supportsThreadFork = MutableStateFlow(true)
    val supportsThreadFork: StateFlow<Boolean> = _supportsThreadFork.asStateFlow()
    private var pendingNotificationOpenThreadID: String? = null
    private var pendingNotificationOpenTurnID: String? = null
    private var runCompletionNotificationDedupedAt: MutableMap<String, Long> = mutableMapOf()
    private var bufferedReplayRefreshPending = false

    // --- Initialize ---

    init {
        val loadedMessages = messagePersistence.load().mapValues { (_, messages) ->
            messages.map { message ->
                if (message.isStreaming) {
                    message.copy(isStreaming = false)
                } else {
                    message
                }
            }
        }
        _messagesByThread.value = loadedMessages

        // Listen for incoming messages
        scope.launch {
            connectionManager.incomingMessages.collect { text ->
                incomingRouter.processWireMessage(text)
            }
        }

        scope.launch {
            var previousConnected: Boolean? = null
            connectionManager.isConnected.collect { connected ->
                val lastConnected = previousConnected
                previousConnected = connected
                if (lastConnected == connected) {
                    return@collect
                }
                if (connected) {
                    handleUnderlyingSocketConnected()
                } else if (lastConnected != null) {
                    handleUnderlyingSocketDisconnected()
                }
            }
        }

        // React to secure connection state
        scope.launch {
            secureTransport.state.collect { state ->
                when (state) {
                    CodexSecureConnectionState.CONNECTED_ENCRYPTED -> {
                        currentConnectionTarget = currentConnectionTarget?.copy(
                            nextHandshakeMode = HANDSHAKE_MODE_TRUSTED_RECONNECT
                        )
                        connectionPresentationCoordinator.setBootstrapping(true)
                        initializeSession()
                    }
                    CodexSecureConnectionState.DISCONNECTED -> {
                        supportsStructuredSkillInput = true
                        supportsTurnCollaborationMode = false
                        _supportsThreadFork.value = true
                        connectionPresentationCoordinator.clearTransientLoading()
                        threadHistoryHydrator.reset()
                        stopSyncLoops()
                    }
                    CodexSecureConnectionState.HANDSHAKING -> Unit
                    CodexSecureConnectionState.CONNECTING -> Unit
                    CodexSecureConnectionState.ERROR -> {
                        supportsStructuredSkillInput = true
                        supportsTurnCollaborationMode = false
                        _supportsThreadFork.value = true
                        connectionPresentationCoordinator.clearTransientLoading()
                        threadHistoryHydrator.reset()
                        stopSyncLoops()
                        messageTransport.cancelAllPending("Secure transport error")
                    }
                }
            }
        }

        scope.launch {
            connectionManager.reconnectStatus.collect { reconnectStatus ->
                connectionPresentationCoordinator.applyReconnectStatus(reconnectStatus)
            }
        }

        // Load access mode from store
        secureStore.readString(SecureStore.SELECTED_ACCESS_MODE)?.let { mode ->
            _selectedAccessMode.value = try {
                CodexAccessMode.valueOf(mode)
            } catch (_: Exception) { CodexAccessMode.ON_REQUEST }
        }
        _selectedModel.value = secureStore.readString(SecureStore.SELECTED_MODEL_ID)?.trim()?.takeIf { it.isNotEmpty() }
        _selectedReasoningEffort.value = secureStore
            .readString(SecureStore.SELECTED_REASONING_EFFORT)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
        _selectedServiceTier.value = secureStore
            .readString(SecureStore.SELECTED_SERVICE_TIER)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let {
                runCatching { CodexServiceTier.valueOf(it) }.getOrNull()
            }
        _activeThreadId.value = persistedActiveThreadId()
        runCompletionNotifier.refreshState()
        secureStore.readCodable(
            SecureStore.AI_CHANGE_SET_LEDGER,
            AIChangeSetLedgerSnapshot.serializer()
        )?.let(::restoreAIChangeSetLedger)
    }

    // --- Connection ---

    fun connect(relayUrl: String, sessionId: String, handshakeMode: String = "qr_bootstrap") {
        currentConnectionTarget = RelayConnectionTarget(
            relayUrl = relayUrl,
            sessionId = sessionId,
            nextHandshakeMode = handshakeMode
        )
        connectionPresentationCoordinator.prepareForConnection(
            isTrustedReconnect = handshakeMode == HANDSHAKE_MODE_TRUSTED_RECONNECT
        )
        threadHistoryHydrator.reset()
        stopSyncLoops()
        messageTransport.cancelAllPending("Starting new connection")
        connectionManager.resetReconnectState()
        secureTransport.reset()
        connectionManager.connect(relayUrl, sessionId)
    }

    fun connectFromSavedRelay() {
        val url = secureStore.readString(SecureStore.RELAY_URL) ?: return
        val sessionId = secureStore.readString(SecureStore.RELAY_SESSION_ID) ?: return
        connect(url, sessionId, "trusted_reconnect")
    }

    fun connectFromQR(payload: CodexPairingQRPayload) {
        // Save relay info
        secureStore.writeString(SecureStore.RELAY_URL, payload.relay)
        secureStore.writeString(SecureStore.RELAY_SESSION_ID, payload.sessionId)
        secureStore.writeString(SecureStore.RELAY_MAC_DEVICE_ID, payload.macDeviceId)
        secureStore.writeString(SecureStore.RELAY_MAC_IDENTITY_PUBLIC_KEY, payload.macIdentityPublicKey)
        secureStore.deleteValue(SecureStore.LAST_APPLIED_BRIDGE_OUTBOUND_SEQ)
        secureStore.deleteValue(SecureStore.LAST_APPLIED_BRIDGE_REPLAY_EPOCH)

        connect(payload.relay, payload.sessionId, "qr_bootstrap")
    }

    fun disconnect() {
        currentConnectionTarget = null
        stopSyncLoops()
        messageTransport.cancelAllPending("Disconnected")
        connectionManager.disconnect()
        connectionManager.resetReconnectState()
        secureTransport.reset()
        assistantHandler.clearStreamingState()
        connectionPresentationCoordinator.resetForManualDisconnect()
        threadHistoryHydrator.reset()
        pendingGPTLoginId = null
        _gptAccountSnapshot.value = _gptAccountSnapshot.value.copy(
            status = CodexGPTAccountStatus.UNAVAILABLE,
            loginInFlight = false,
            needsReauth = false,
            updatedAtEpochMs = System.currentTimeMillis()
        )
    }

    private fun handleUnderlyingSocketConnected() {
        val target = currentConnectionTarget ?: return
        val secureState = secureTransport.state.value
        if (secureState == CodexSecureConnectionState.HANDSHAKING
            || secureState == CodexSecureConnectionState.CONNECTED_ENCRYPTED
        ) {
            return
        }

        Log.d(TAG, "Starting secure handshake (${target.nextHandshakeMode})")
        val clientHello = secureTransport.buildClientHello(target.sessionId, target.nextHandshakeMode)
        val helloText = json.encodeToString(SecureClientHello.serializer(), clientHello)
        if (!connectionManager.send(helloText)) {
            Log.e(TAG, "Failed to send clientHello after socket connect")
            messageTransport.cancelAllPending("Handshake send failed")
            secureTransport.reset()
        }
    }

    private fun handleUnderlyingSocketDisconnected() {
        supportsStructuredSkillInput = true
        supportsTurnCollaborationMode = false
        _supportsThreadFork.value = true
        connectionPresentationCoordinator.clearTransientLoading()
        threadHistoryHydrator.reset()
        stopSyncLoops()
        messageTransport.cancelAllPending("Connection lost")
        assistantHandler.clearStreamingState()

        if (secureTransport.state.value != CodexSecureConnectionState.ERROR
            && secureTransport.state.value != CodexSecureConnectionState.DISCONNECTED
        ) {
            secureTransport.reset()
        }
    }

    fun hasSavedRelay(): Boolean =
        secureStore.readString(SecureStore.RELAY_URL) != null
                && secureStore.readString(SecureStore.RELAY_SESSION_ID) != null

    fun isRuntimeReady(): Boolean = connectionPhase.value == CodexConnectionPhase.CONNECTED

    fun isThreadHistoryLoading(threadId: String?): Boolean {
        val normalizedThreadId = threadId?.trim()?.takeIf { it.isNotEmpty() } ?: return false
        return threadHistoryHydrator.isLoading(normalizedThreadId)
    }

    fun trustedPairPresentation(): CodexTrustedPairPresentation? {
        val visibleTrustedMac = visibleTrustedMacRecord()
        val trustedPairDeviceId = normalizedRelayMacDeviceId() ?: visibleTrustedMac?.macDeviceId
        val macName = visibleTrustedMac?.displayName?.trim()?.takeIf { it.isNotEmpty() }
        val macFingerprint = normalizedRelayMacIdentityPublicKey()?.let(::codexSecureFingerprint)
            ?: visibleTrustedMac?.macIdentityPublicKey?.let(::codexSecureFingerprint)

        if (macName == null && macFingerprint == null) {
            return null
        }

        val fallbackName = "Mac ${macFingerprint ?: ""}".trim()
        val systemName = macName ?: fallbackName
        val nickname = trustedPairNickname(trustedPairDeviceId)
        val effectiveName = nickname.takeIf { it.isNotBlank() } ?: systemName

        return CodexTrustedPairPresentation(
            deviceId = trustedPairDeviceId,
            title = trustedPairTitle(),
            name = effectiveName,
            systemName = systemName.takeIf { nickname.isNotBlank() },
            detail = trustedPairDetail(displayName = macName, fingerprint = macFingerprint),
            nickname = nickname
        )
    }

    fun setTrustedPairNickname(deviceId: String?, nickname: String) {
        val storageKey = trustedMacNicknameStorageKey(deviceId) ?: return
        val trimmed = nickname.trim()
        if (trimmed.isEmpty()) {
            secureStore.deleteValue(storageKey)
        } else {
            secureStore.writeString(storageKey, trimmed)
        }
    }

    fun forgetPairing() {
        disconnect()
        secureStore.deleteValue(SecureStore.RELAY_URL)
        secureStore.deleteValue(SecureStore.RELAY_SESSION_ID)
        secureStore.deleteValue(SecureStore.RELAY_MAC_DEVICE_ID)
        secureStore.deleteValue(SecureStore.RELAY_MAC_IDENTITY_PUBLIC_KEY)
        secureStore.deleteValue(SecureStore.LAST_APPLIED_BRIDGE_OUTBOUND_SEQ)
        secureStore.deleteValue(SecureStore.LAST_APPLIED_BRIDGE_REPLAY_EPOCH)
    }

    // --- Session Init ---

    private fun initializeSession() {
        scope.launch {
            try {
                if (secureTransport.consumeReplayRefreshRequired()) {
                    threadHistoryHydrator.reset()
                }
                connectionPresentationCoordinator.setBootstrapping(true)
                supportsStructuredSkillInput = true
                supportsTurnCollaborationMode = false
                _supportsThreadFork.value = true
                val clientInfo = JsonValue.obj(
                    "name" to JsonValue.string("remodex_android"),
                    "title" to JsonValue.string("Remodex Android"),
                    "version" to JsonValue.string(APP_VERSION)
                )
                val modernParams = JsonValue.obj(
                    "clientInfo" to clientInfo,
                    "capabilities" to JsonValue.obj(
                        "experimentalApi" to JsonValue.bool(true)
                    )
                )
                val legacyParams = JsonValue.obj(
                    "clientInfo" to clientInfo
                )

                try {
                    requireSuccessfulResponse(
                        messageTransport.sendRequest("initialize", modernParams),
                        "initialize"
                    )
                    supportsTurnCollaborationMode = runtimeSupportsPlanCollaborationMode()
                } catch (e: Exception) {
                    if (!shouldRetryInitializeWithoutCapabilities(e)) {
                        throw e
                    }
                    Log.w(TAG, "Retrying initialize without capabilities: ${e.message}")
                    requireSuccessfulResponse(
                        messageTransport.sendRequest("initialize", legacyParams),
                        "initialize"
                    )
                    supportsTurnCollaborationMode = false
                }

                messageTransport.sendNotification("initialized")
                Log.d(TAG, "Session initialized")
                performPostConnectSyncPass()
            } catch (e: Exception) {
                connectionPresentationCoordinator.setBootstrapping(false)
                Log.e(TAG, "Initialize failed: ${e.message}")
            }
        }
    }

    private suspend fun performPostConnectSyncPass() {
        postConnectSyncCoordinator.run()
    }

    private fun visibleTrustedMacRecord(): CodexTrustedMacRecord? {
        val registry = secureTransport.getTrustedMacRegistry()
        val relayDeviceId = normalizedRelayMacDeviceId()
        if (relayDeviceId != null) {
            registry.macs[relayDeviceId]?.let { return it }
        }

        return registry.macs.values.maxByOrNull { it.lastConnectedAt }
    }

    private fun normalizedRelayMacDeviceId(): String? =
        secureStore.readString(SecureStore.RELAY_MAC_DEVICE_ID)?.trim()?.takeIf { it.isNotEmpty() }

    private fun normalizedRelayMacIdentityPublicKey(): String? =
        secureStore.readString(SecureStore.RELAY_MAC_IDENTITY_PUBLIC_KEY)?.trim()?.takeIf { it.isNotEmpty() }

    private fun trustedPairTitle(): String {
        return when {
            isConnected.value && secureTransport.state.value == CodexSecureConnectionState.CONNECTED_ENCRYPTED ->
                "Connected Pair"
            isConnecting.value || secureTransport.state.value == CodexSecureConnectionState.HANDSHAKING ->
                "Pairing Mac"
            hasSavedRelay() || visibleTrustedMacRecord() != null ->
                "Saved Pair"
            else -> "Trusted Pair"
        }
    }

    private fun trustedPairDetail(displayName: String?, fingerprint: String?): String? {
        val parts = mutableListOf(trustedPairConnectionStatusLabel())
        if (displayName != null && !fingerprint.isNullOrBlank()) {
            parts += fingerprint
        }
        return parts.joinToString(" · ").ifBlank { null }
    }

    private fun trustedPairConnectionStatusLabel(): String {
        return when (connectionPhase.value) {
            CodexConnectionPhase.OFFLINE -> "offline"
            CodexConnectionPhase.CONNECTING -> "connecting"
            CodexConnectionPhase.HANDSHAKING -> "pairing"
            CodexConnectionPhase.LOADING_CHATS -> "loading chats"
            CodexConnectionPhase.SYNCING -> "syncing"
            CodexConnectionPhase.CONNECTED -> "connected"
        }
    }

    private fun trustedPairNickname(deviceId: String?): String {
        val storageKey = trustedMacNicknameStorageKey(deviceId) ?: return ""
        return secureStore.readString(storageKey)?.trim().orEmpty()
    }

    private fun trustedMacNicknameStorageKey(deviceId: String?): String? {
        val normalizedDeviceId = deviceId?.trim()?.takeIf { it.isNotEmpty() } ?: return null
        return SecureStore.SIDEBAR_MAC_NICKNAME_PREFIX + normalizedDeviceId
    }

    private fun codexSecureFingerprint(publicKeyBase64: String): String {
        val decoded = try {
            Base64.decode(publicKeyBase64, Base64.DEFAULT)
        } catch (_: Exception) {
            ByteArray(0)
        }
        if (decoded.isEmpty()) {
            return "INVALID"
        }

        val digest = MessageDigest.getInstance("SHA-256").digest(decoded)
        return digest.joinToString(separator = "") { byte ->
            "%02x".format(byte)
        }.take(12).uppercase()
    }

    private fun shouldRetryInitializeWithoutCapabilities(error: Throwable): Boolean {
        val message = error.message.orEmpty().lowercase()

        if (!message.contains("capabilities") && !message.contains("experimentalapi")) {
            return false
        }

        return message.contains("unknown")
            || message.contains("unexpected")
            || message.contains("unrecognized")
            || message.contains("invalid")
            || message.contains("unsupported")
            || message.contains("field")
    }

    private suspend fun runtimeSupportsPlanCollaborationMode(): Boolean {
        return try {
            responseContainsPlanCollaborationMode(
                requireSuccessfulResponse(
                    messageTransport.sendRequest("collaborationMode/list"),
                    "collaborationMode/list"
                )
            )
        } catch (_: Exception) {
            false
        }
    }

    private fun responseContainsPlanCollaborationMode(response: RpcMessage): Boolean {
        val candidateArrays = listOf(
            response.result?.arrayValue,
            response.result?.objectValue?.get("modes")?.arrayValue,
            response.result?.objectValue?.get("collaborationModes")?.arrayValue,
            response.result?.objectValue?.get("items")?.arrayValue
        )

        candidateArrays.forEach { entries ->
            entries?.forEach { entry ->
                val modeName = entry.objectValue?.get("mode")?.stringValue
                    ?: entry.objectValue?.get("name")?.stringValue
                    ?: entry.objectValue?.get("id")?.stringValue
                    ?: entry.stringValue
                if (modeName == CodexCollaborationModeKind.PLAN.wireValue) {
                    return true
                }
            }
        }

        return false
    }

    private fun requireSuccessfulResponse(response: RpcMessage, method: String): RpcMessage {
        val rpcError = response.error ?: return response
        throw RpcRequestException(
            method = method,
            code = rpcError.code,
            errorCode = rpcError.errorCode,
            rpcMessage = rpcError.message
        )
    }

    private class RpcRequestException(
        val method: String,
        val code: Int,
        val errorCode: String?,
        val rpcMessage: String
    ) : IllegalStateException("RPC $method failed ($code): $rpcMessage")

    private class GitRpcException(
        val errorCode: String?,
        override val message: String
    ) : IllegalStateException(message)

    private fun gitUserMessage(errorCode: String?, fallback: String?): String {
        return when (errorCode) {
            "nothing_to_commit" -> "Nothing to commit."
            "nothing_to_push" -> "Nothing to push."
            "push_rejected" -> "Push rejected. Pull changes first."
            "branch_is_main" -> "Cannot operate on the main branch."
            "protected_branch" -> "This branch is protected."
            "branch_behind_remote" -> "Branch is behind remote. Pull first."
            "dirty_and_behind" -> "Uncommitted changes and branch is behind remote."
            "checkout_conflict_dirty_tree" -> "Cannot switch branches: you have uncommitted changes."
            "checkout_branch_in_other_worktree" ->
                "Cannot switch branches: this branch is already open in another worktree."
            "pull_conflict" -> "Pull failed due to conflicts."
            "branch_exists" -> fallback ?: "Branch already exists."
            "invalid_branch_name" -> fallback ?: "Branch name is not valid for Git."
            "missing_branch", "missing_branch_name" -> "Branch name is required."
            "missing_base_branch" -> fallback ?: "Base branch is required."
            "branch_already_open_here" -> fallback ?: "This branch is already open in the current project."
            "branch_in_other_worktree" -> fallback ?: "This branch is already open in another worktree."
            "dirty_worktree_base_mismatch" -> fallback
                ?: "Tracked local changes can move into a new worktree only from the current branch."
            "confirmation_required" -> "Confirmation is required for this action."
            "stash_pop_conflict" -> "Stash pop failed due to conflicts."
            "missing_local_repo" -> "Run `remodex up` from an existing local directory first."
            "missing_working_directory" -> fallback ?: "The selected local folder is not available on this Mac."
            "cannot_remove_local_checkout" -> fallback ?: "Cannot remove the main local checkout."
            "unmanaged_worktree" -> fallback ?: "Only managed worktrees can be cleaned up automatically."
            "worktree_cleanup_failed" -> fallback ?: "We could not clean up the temporary worktree automatically."
            "create_worktree_failed" -> fallback ?: "Could not create worktree."
            else -> fallback ?: "Git operation failed."
        }
    }

    private fun desktopHandoffUserMessage(errorCode: String?, fallback: String?): String {
        return when (errorCode) {
            "missing_thread_id" -> "This chat does not have a valid thread id yet."
            "unsupported_platform" -> "Mac handoff works only when the bridge is running on macOS."
            "handoff_failed" -> fallback ?: "Could not relaunch Codex.app on your Mac."
            else -> fallback ?: "Could not continue this chat on your Mac."
        }
    }

    private suspend fun sendGitRequestResult(
        method: String,
        threadId: String? = null,
        cwd: String? = null,
        extra: List<Pair<String, JsonValue>> = emptyList()
    ): JsonValue {
        val params = gitRequestParams(threadId = threadId, cwd = cwd, extra = extra)
            ?: throw GitRpcException(
                errorCode = "missing_working_directory",
                message = gitUserMessage("missing_working_directory", null)
            )
        val response = messageTransport.sendRequest(method, params)
        response.error?.let { rpcError ->
            throw GitRpcException(
                errorCode = rpcError.errorCode,
                message = gitUserMessage(rpcError.errorCode, rpcError.message.ifBlank { null })
            )
        }
        return response.result ?: JsonValue.NullValue
    }

    private suspend fun refreshModelsNow() {
        connectionPresentationCoordinator.setLoadingModels(true)
        try {
            val response = requireSuccessfulResponse(
                messageTransport.sendRequest(
                    "model/list",
                    JsonValue.obj(
                        "cursor" to JsonValue.NullValue,
                        "limit" to JsonValue.int(50),
                        "includeHidden" to JsonValue.bool(false)
                    )
                ),
                "model/list"
            )
            val result = response.result?.objectValue ?: return
            val items = result["items"]?.arrayValue
                ?: result["data"]?.arrayValue
                ?: result["models"]?.arrayValue
                ?: emptyList()
            val models = items.mapNotNull { modelValue ->
                try {
                    json.decodeFromString(
                        CodexModelOption.serializer(),
                        json.encodeToString(JsonValue.serializer(), modelValue)
                    )
                } catch (_: Exception) {
                    null
                }
            }
            if (models.isNotEmpty()) {
                _availableModels.value = models
                normalizeRuntimeSelectionsAfterModelsUpdate()
            }
        } catch (e: Exception) {
            Log.w(TAG, "Model list refresh failed: ${e.message}")
        } finally {
            connectionPresentationCoordinator.setLoadingModels(false)
        }
    }

    fun refreshAccountStatus() {
        scope.launch {
            refreshAccountStatusNow()
        }
    }

    suspend fun transcribeVoiceClip(
        wavData: ByteArray,
        durationMs: Long
    ): String {
        if (!isConnected.value) {
            throw IllegalStateException("Connect to your Mac before using voice transcription.")
        }

        CodexVoiceTranscriptionPreflight(
            byteCount = wavData.size,
            durationMs = durationMs
        ).validate()
        Log.d(TAG, "Starting voice transcription durationMs=$durationMs bytes=${wavData.size}")

        try {
            val token = resolveVoiceAuthToken()
            val transcript = GPTVoiceTranscriptionClient.transcribe(
                okHttpClient = okHttpClient,
                json = json,
                wavData = wavData,
                token = token
            )
            _gptAccountErrorMessage.value = null
            Log.d(TAG, "Voice transcription succeeded with ${transcript.length} chars")
            return transcript.trim()
        } catch (error: VoiceTranscriptionAuthExpiredException) {
            Log.w(TAG, "Voice transcription auth expired, refreshing account status")
            refreshAccountStatusNow()
            val freshToken = resolveVoiceAuthToken()
            val transcript = GPTVoiceTranscriptionClient.transcribe(
                okHttpClient = okHttpClient,
                json = json,
                wavData = wavData,
                token = freshToken
            )
            _gptAccountErrorMessage.value = null
            Log.d(TAG, "Voice transcription succeeded after reauth with ${transcript.length} chars")
            return transcript.trim()
        } catch (error: Exception) {
            Log.w(TAG, "Voice transcription failed: ${error.message}", error)
            throw error
        }
    }

    private suspend fun resolveVoiceAuthToken(): String {
        return try {
            Log.d(TAG, "Resolving voice auth token")
            val response = requireSuccessfulResponse(
                messageTransport.sendRequest("voice/resolveAuth"),
                "voice/resolveAuth"
            )
            val payload = response.result?.objectValue
                ?: throw IllegalStateException("voice/resolveAuth did not return a valid token")
            val token = payload["token"]?.stringValue?.trim().orEmpty()
            if (token.isEmpty()) {
                throw IllegalStateException("voice/resolveAuth did not return a valid token")
            }
            Log.d(TAG, "Resolved voice auth token")
            token
        } catch (error: RpcRequestException) {
            if (error.errorCode == "not_authenticated") {
                refreshAccountStatusNow()
                throw VoiceTranscriptionAuthExpiredException()
            }
            Log.w(TAG, "voice/resolveAuth failed: ${error.rpcMessage}")
            throw IllegalStateException(error.rpcMessage.ifBlank { "Voice transcription failed." })
        }
    }

    private suspend fun refreshAccountStatusNow() {
        try {
            val result = messageTransport.sendRequest("account/status/read", JsonValue.obj(
                "refreshToken" to JsonValue.bool(false),
                "includeToken" to JsonValue.bool(false)
            ))
            val data = result.result?.objectValue ?: return
            _bridgeVersionInfo.value =
                data["bridgeVersionInfo"]?.objectValue?.get("version")?.stringValue
                    ?: firstStringValue(
                        data,
                        "bridgeVersion",
                        "bridge_version",
                        "bridgePackageVersion",
                        "bridge_package_version"
                    )
                    ?: _bridgeVersionInfo.value

            val snapshot = decodeBridgeGPTAccountSnapshot(data)
            _gptAccountSnapshot.value = snapshot
            if (snapshot.isAuthenticated || snapshot.status == CodexGPTAccountStatus.NOT_LOGGED_IN) {
                _gptAccountErrorMessage.value = null
            }
        } catch (e: Exception) {
            Log.w(TAG, "Account status refresh failed: ${e.message}")
        }
    }

    suspend fun fuzzyFileSearch(
        query: String,
        roots: List<String>,
        cancellationToken: String? = null
    ): List<CodexFuzzyFileMatch> {
        val normalizedQuery = query.trim()
        if (normalizedQuery.isEmpty()) {
            return emptyList()
        }

        val normalizedRoots = roots
            .map { it.trim() }
            .filter { it.isNotEmpty() }
        if (normalizedRoots.isEmpty()) {
            return emptyList()
        }

        val params = mutableMapOf<String, JsonValue>(
            "query" to JsonValue.string(normalizedQuery),
            "roots" to JsonValue.array(*normalizedRoots.map(JsonValue::string).toTypedArray())
        )
        cancellationToken?.trim()?.takeIf { it.isNotEmpty() }?.let {
            params["cancellationToken"] = JsonValue.string(it)
        } ?: run {
            params["cancellationToken"] = JsonValue.NullValue
        }

        val response = requireSuccessfulResponse(
            messageTransport.sendRequest("fuzzyFileSearch", JsonValue.ObjectValue(params)),
            "fuzzyFileSearch"
        )

        val decodedFiles = decodeFuzzyFileMatches(response.result)
            ?: throw IllegalStateException("fuzzyFileSearch response missing result.files")

        return decodedFiles.map { match ->
            val normalizedPath = normalizeFuzzyFilePath(match.path, match.root.orEmpty())
            match.copy(path = normalizedPath)
        }
    }

    suspend fun listSkills(
        cwds: List<String>?,
        forceReload: Boolean = false
    ): List<CodexSkillMetadata> {
        val normalizedCwds = (cwds ?: emptyList())
            .map { it.trim() }
            .filter { it.isNotEmpty() }
        var params = mutableMapOf<String, JsonValue>()
        if (normalizedCwds.isNotEmpty()) {
            params["cwds"] = JsonValue.array(*normalizedCwds.map(JsonValue::string).toTypedArray())
        }
        if (forceReload) {
            params["forceReload"] = JsonValue.bool(true)
        }

        val response = try {
            requireSuccessfulResponse(
                messageTransport.sendRequest("skills/list", JsonValue.ObjectValue(params)),
                "skills/list"
            )
        } catch (error: Exception) {
            if (normalizedCwds.isEmpty() || !shouldRetrySkillsListWithCwdFallback(error)) {
                throw error
            }

            val fallbackParams = mutableMapOf<String, JsonValue>(
                "cwd" to JsonValue.string(normalizedCwds.first())
            )
            if (forceReload) {
                fallbackParams["forceReload"] = JsonValue.bool(true)
            }
            requireSuccessfulResponse(
                messageTransport.sendRequest("skills/list", JsonValue.ObjectValue(fallbackParams)),
                "skills/list"
            )
        }

        val decodedSkills = decodeSkillMetadata(response.result)
            ?: throw IllegalStateException("skills/list response missing result.data[].skills")

        return decodedSkills
            .groupBy { it.normalizedName }
            .values
            .mapNotNull { bucket ->
                bucket.firstOrNull { it.enabled } ?: bucket.firstOrNull()
            }
            .filter { it.name.trim().isNotEmpty() }
            .sortedBy { it.name.lowercase() }
    }

    // --- Thread Operations ---

    fun selectThread(threadId: String?) {
        val normalizedThreadId = threadId?.trim()?.takeIf { it.isNotEmpty() }
        _activeThreadId.value = normalizedThreadId
        persistActiveThreadId(normalizedThreadId)
        if (normalizedThreadId != null) {
            requestThreadHistoryLoad(normalizedThreadId)
        }
    }

    fun setAppInForeground(isForeground: Boolean) {
        Log.d(TAG, "setAppInForeground($isForeground)")
        isAppInForeground = isForeground
        syncBackgroundTurnMonitoring()
    }

    fun refreshNotificationPermissionState() {
        runCompletionNotifier.refreshState()
    }

    fun backgroundTurnMonitorCount(): Int =
        if (runCompletionNotifier.canPostNotifications()) {
            _runningThreadIDs.value.size
        } else {
            0
        }

    fun startNewThread(projectPath: String? = null) {
        scope.launch {
            val preferredProjectPath = CodexThread.normalizeProjectPath(projectPath)
            if (!isConnected.value) {
                Log.w(TAG, "Ignoring new thread request while disconnected")
                return@launch
            }
            if (!isRuntimeReady()) {
                Log.w(TAG, "Ignoring new thread request before runtime is ready (${connectionPhase.value})")
                return@launch
            }

            try {
                val params = mutableMapOf<String, JsonValue>()
                preferredProjectPath?.let { params["cwd"] = JsonValue.string(it) }
                runtimeModelIdentifierForTurn()?.let { params["model"] = JsonValue.string(it) }
                _selectedServiceTier.value?.let { params["serviceTier"] = JsonValue.string(it.name.lowercase()) }
                Log.d(TAG, "Starting new thread")
                val result = requireSuccessfulResponse(
                    messageTransport.sendRequest("thread/start", JsonValue.ObjectValue(params)),
                    "thread/start"
                )
                val resultObject = result.result?.objectValue
                val threadValue = resultObject?.get("thread")
                val decodedThread = threadValue
                    ?.let { CodexThread.fromJson(json, it.toJsonElement()) }
                    ?.let { applyPreferredProjectFallback(it, preferredProjectPath) }
                val threadId = resultObject?.get("threadId")?.stringValue
                    ?: resultObject?.get("thread_id")?.stringValue
                    ?: resultObject?.get("id")?.stringValue
                    ?: decodedThread?.id

                if (decodedThread != null) {
                    upsertThread(decodedThread)
                } else if (threadId != null && preferredProjectPath != null) {
                    upsertThread(CodexThread(id = threadId, cwd = preferredProjectPath))
                }
                if (threadId != null) {
                    if (decodedThread == null) {
                        syncThreadList()
                    }
                    selectThread(threadId)
                } else {
                    Log.w(TAG, "thread/start response missing thread id")
                }
            } catch (e: Exception) {
                Log.e(TAG, "Start thread failed: ${e.message}")
            }
        }
    }

    suspend fun moveThreadToProjectPath(threadId: String, projectPath: String): CodexThread {
        val normalizedThreadId = threadId.trim().takeIf { it.isNotEmpty() }
            ?: throw IllegalArgumentException("A thread id is required.")
        val normalizedProjectPath = CodexThread.normalizeProjectPath(projectPath)
            ?: throw IllegalArgumentException("A valid project path is required.")
        val currentThread = thread(normalizedThreadId)
            ?: throw IllegalArgumentException("Thread not found.")
        val previousThread = currentThread
        val previousPersistedProjectPath = persistedThreadProjectPath(normalizedThreadId)
        val now = (System.currentTimeMillis() / 1000.0).toString()

        persistThreadProjectPath(normalizedProjectPath, normalizedThreadId)
        upsertThread(currentThread.copy(cwd = normalizedProjectPath, updatedAtRaw = now))
        selectThread(normalizedThreadId)

        return try {
            val resumedThread = ensureThreadResumed(
                threadId = normalizedThreadId,
                preferredProjectPath = normalizedProjectPath,
                sourceProjectPath = previousThread.gitWorkingDirectory,
                authoritativeForkProjectPath = normalizedProjectPath
            )
            val authoritativeThread = applyPreferredProjectFallback(
                resumedThread ?: thread(normalizedThreadId) ?: previousThread,
                normalizedProjectPath
            ).copy(
                cwd = normalizedProjectPath,
                updatedAtRaw = now
            )
            upsertThread(authoritativeThread)
            val resolvedThread = (thread(normalizedThreadId) ?: authoritativeThread).let { latestThread ->
                if (latestThread.projectKey == normalizedProjectPath) {
                    latestThread
                } else {
                    latestThread.copy(cwd = normalizedProjectPath, updatedAtRaw = now)
                }
            }
            upsertThread(resolvedThread)
            selectThread(normalizedThreadId)
            requestImmediateActiveThreadSync(normalizedThreadId)
            thread(normalizedThreadId) ?: resolvedThread
        } catch (error: Exception) {
            if (previousPersistedProjectPath != null) {
                persistThreadProjectPath(previousPersistedProjectPath, normalizedThreadId)
            } else {
                removePersistedThreadProjectPath(normalizedThreadId)
            }
            upsertThread(previousThread)
            selectThread(normalizedThreadId)
            throw error
        }
    }

    suspend fun forkThread(
        threadId: String,
        targetProjectPath: String? = null
    ): CodexThread {
        val normalizedThreadId = threadId.trim()
        require(normalizedThreadId.isNotEmpty()) { "A source thread id is required." }

        if (!isConnected.value) {
            throw IllegalStateException("Connect to runtime first.")
        }
        if (!isRuntimeReady()) {
            throw IllegalStateException("Runtime is still initializing. Wait a moment and retry.")
        }

        val sourceThread = thread(normalizedThreadId)
            ?: throw IllegalArgumentException("Thread not found.")
        val normalizedTargetProjectPath = CodexThread.normalizeProjectPath(targetProjectPath)
        val preferredModelIdentifier = sourceThread.model?.trim()?.takeIf { it.isNotEmpty() }
            ?: runtimeModelIdentifierForTurn()
        var usesMinimalForkParams = false
        var includesServiceTier = _selectedServiceTier.value != null
        var includesSandbox = true

        while (true) {
            val params = makeThreadForkParams(
                sourceThreadId = normalizedThreadId,
                sourceThread = sourceThread,
                targetProjectPath = normalizedTargetProjectPath,
                includeServiceTier = includesServiceTier,
                includeSandbox = includesSandbox,
                usesMinimalForkParams = usesMinimalForkParams
            )

            val response = try {
                sendRequestWithApprovalPolicyFallback(
                    method = "thread/fork",
                    baseParams = params,
                    context = if (includesSandbox) "sandbox" else "minimal"
                )
            } catch (error: Exception) {
                if (shouldTreatAsUnsupportedThreadFork(error)) {
                    _supportsThreadFork.value = false
                    throw IllegalStateException(
                        "This Mac bridge does not support native thread forks yet. Update Remodex on your Mac and retry."
                    )
                }
                if (!usesMinimalForkParams && shouldRetryThreadForkWithoutOverrides(error)) {
                    usesMinimalForkParams = true
                    includesServiceTier = false
                    includesSandbox = false
                    continue
                }
                if (includesServiceTier && shouldRetryWithoutServiceTier(error)) {
                    includesServiceTier = false
                    continue
                }
                if (includesSandbox && shouldFallbackFromSandboxPolicy(error)) {
                    includesSandbox = false
                    continue
                }
                throw error
            }

            return handleThreadForkResponse(
                response = response,
                sourceThreadId = normalizedThreadId,
                sourceProjectPath = sourceThread.gitWorkingDirectory,
                fallbackProjectPath = normalizedTargetProjectPath ?: sourceThread.gitWorkingDirectory,
                preferredModelIdentifier = preferredModelIdentifier,
                usesPostForkResumeOverrides = usesMinimalForkParams
            )
        }
    }

    private fun makeThreadForkParams(
        sourceThreadId: String,
        sourceThread: CodexThread,
        targetProjectPath: String?,
        includeServiceTier: Boolean,
        includeSandbox: Boolean,
        usesMinimalForkParams: Boolean
    ): MutableMap<String, JsonValue> {
        val params = mutableMapOf<String, JsonValue>(
            "threadId" to JsonValue.string(sourceThreadId)
        )

        if (usesMinimalForkParams) {
            return params
        }

        targetProjectPath?.let { params["cwd"] = JsonValue.string(it) }
        (sourceThread.model?.trim()?.takeIf { it.isNotEmpty() } ?: runtimeModelIdentifierForTurn())
            ?.let { params["model"] = JsonValue.string(it) }
        sourceThread.modelProvider?.trim()?.takeIf { it.isNotEmpty() }
            ?.let { params["modelProvider"] = JsonValue.string(it) }
        if (includeServiceTier) {
            _selectedServiceTier.value?.let { params["serviceTier"] = JsonValue.string(it.name.lowercase()) }
        }
        if (includeSandbox) {
            params["sandbox"] = JsonValue.string(_selectedAccessMode.value.sandboxLegacyValue)
        }

        return params
    }

    private fun applyPreferredProjectFallback(
        thread: CodexThread,
        preferredProjectPath: String?
    ): CodexThread {
        if (thread.projectKey != null || preferredProjectPath == null) {
            return thread
        }

        return thread.copy(cwd = preferredProjectPath)
    }

    fun renameThread(threadId: String, name: String) {
        val trimmedName = name.trim()
        if (trimmedName.isEmpty()) return

        upsertThread(
            thread(threadId)?.copy(title = trimmedName, name = trimmedName)
                ?: CodexThread(id = threadId, title = trimmedName, name = trimmedName)
        )
        persistThreadRename(trimmedName, threadId)

        scope.launch {
            try {
                requireSuccessfulResponse(
                    messageTransport.sendRequest(
                        "thread/name/set",
                        JsonValue.obj(
                            "thread_id" to JsonValue.string(threadId),
                            "name" to JsonValue.string(trimmedName)
                        )
                    ),
                    "thread/name/set"
                )
            } catch (e: Exception) {
                Log.e(TAG, "Rename thread failed: ${e.message}")
            }
        }
    }

    fun archiveThread(threadId: String) {
        val subtreeThreadIDs = collectSubtreeThreadIDs(threadId)
        subtreeThreadIDs.forEach { setThreadArchivedLocally(it, isArchived = true) }

        scope.launch {
            try {
                sendThreadArchiveRPC(threadId, unarchive = false)
            } catch (e: Exception) {
                Log.e(TAG, "Archive thread failed: ${e.message}")
            }
        }
    }

    fun unarchiveThread(threadId: String) {
        val subtreeThreadIDs = collectSubtreeThreadIDs(threadId)
        subtreeThreadIDs.forEach { setThreadArchivedLocally(it, isArchived = false) }

        scope.launch {
            try {
                sendThreadArchiveRPC(threadId, unarchive = true)
            } catch (e: Exception) {
                Log.e(TAG, "Unarchive thread failed: ${e.message}")
            }
        }
    }

    fun deleteThread(threadId: String) {
        val descendants = collectDescendantThreadIDs(threadId)
        descendants.forEach { setThreadArchivedLocally(it, isArchived = true) }
        removeThreadLocally(threadId, persistAsDeleted = true)

        scope.launch {
            try {
                sendThreadArchiveRPC(threadId, unarchive = false)
            } catch (e: Exception) {
                Log.e(TAG, "Delete thread failed: ${e.message}")
            }
        }
    }

    suspend fun continueOnMac(threadId: String) {
        val normalizedThreadId = threadId.trim().takeIf { it.isNotEmpty() }
            ?: throw IllegalArgumentException("This chat does not have a valid thread id yet.")
        if (!isConnected.value) {
            throw IllegalStateException("Not connected to your Mac.")
        }

        try {
            val response = requireSuccessfulResponse(
                messageTransport.sendRequest(
                    "desktop/continueOnMac",
                    JsonValue.obj(
                        "threadId" to JsonValue.string(normalizedThreadId)
                    )
                ),
                "desktop/continueOnMac"
            )

            val success = response.result
                ?.objectValue
                ?.get("success")
                ?.boolValue == true
            if (!success) {
                throw IllegalStateException("Could not continue this chat on your Mac.")
            }
        } catch (error: RpcRequestException) {
            throw IllegalStateException(
                desktopHandoffUserMessage(error.errorCode, error.rpcMessage)
            )
        }
    }

    suspend fun startReview(
        threadId: String,
        target: CodexReviewTarget,
        baseBranch: String? = null
    ) {
        val normalizedThreadId = threadId.trim().takeIf { it.isNotEmpty() }
            ?: throw IllegalArgumentException("Choose a review target first.")
        if (!isConnected.value) {
            throw IllegalStateException("Connect to runtime first.")
        }

        val normalizedBaseBranch = baseBranch?.trim()?.takeIf { it.isNotEmpty() }
        if (target == CodexReviewTarget.BASE_BRANCH && normalizedBaseBranch == null) {
            throw IllegalArgumentException("Choose a base branch before starting this review.")
        }

        val reviewPrompt = when (target) {
            CodexReviewTarget.UNCOMMITTED_CHANGES -> "Review current changes"
            CodexReviewTarget.BASE_BRANCH -> "Review against base branch ${normalizedBaseBranch.orEmpty()}".trim()
        }

        val optimisticMsg = CodexMessage(
            threadId = normalizedThreadId,
            role = CodexMessageRole.USER,
            kind = CodexMessageKind.CHAT,
            text = reviewPrompt,
            orderIndex = CodexMessageOrderCounter.next(),
            deliveryState = CodexMessageDeliveryState.PENDING
        )
        appendMessage(normalizedThreadId, optimisticMsg)
        selectThread(normalizedThreadId)

        try {
            val targetPayload = when (target) {
                CodexReviewTarget.UNCOMMITTED_CHANGES -> JsonValue.obj(
                    "type" to JsonValue.string("uncommittedChanges")
                )
                CodexReviewTarget.BASE_BRANCH -> JsonValue.obj(
                    "type" to JsonValue.string("baseBranch"),
                    "branch" to JsonValue.string(normalizedBaseBranch.orEmpty())
                )
            }

            requireSuccessfulResponse(
                sendRequestWithSandboxFallback(
                    method = "review/start",
                    baseParams = mutableMapOf(
                        "threadId" to JsonValue.string(normalizedThreadId),
                        "delivery" to JsonValue.string("inline"),
                        "target" to targetPayload
                    )
                ),
                "review/start"
            )

            updateMessageDeliveryState(
                normalizedThreadId,
                optimisticMsg.id,
                CodexMessageDeliveryState.CONFIRMED
            )
            markThreadRunning(normalizedThreadId)
        } catch (error: Exception) {
            updateMessageDeliveryState(
                normalizedThreadId,
                optimisticMsg.id,
                CodexMessageDeliveryState.FAILED
            )
            clearRunningState(normalizedThreadId)
            appendMessage(
                normalizedThreadId,
                CodexMessage(
                    threadId = normalizedThreadId,
                    role = CodexMessageRole.SYSTEM,
                    kind = CodexMessageKind.CHAT,
                    text = "Review error: ${error.message ?: "Unknown error"}",
                    orderIndex = CodexMessageOrderCounter.next(),
                    deliveryState = CodexMessageDeliveryState.CONFIRMED
                )
            )
            throw error
        }
    }

    fun sendMessage(
        text: String,
        threadId: String? = null,
        attachments: List<CodexImageAttachment> = emptyList(),
        skillMentions: List<CodexTurnSkillMention> = emptyList(),
        collaborationMode: CodexCollaborationModeKind? = null
    ) {
        val tid = threadId ?: _activeThreadId.value ?: return
        if (thread(tid)?.syncState == CodexThreadSyncState.ARCHIVED_LOCAL
            || locallyArchivedThreadIDs().contains(tid)
        ) {
            return
        }
        val trimmedText = text.trim()
        if (trimmedText.isBlank() && attachments.isEmpty()) return

        // Optimistic UI: add pending message
        val optimisticMsg = CodexMessage(
            threadId = tid,
            role = CodexMessageRole.USER,
            kind = CodexMessageKind.CHAT,
            text = trimmedText,
            orderIndex = CodexMessageOrderCounter.next(),
            deliveryState = CodexMessageDeliveryState.PENDING,
            attachments = attachments
        )
        appendMessage(tid, optimisticMsg)

        scope.launch {
            var includeStructuredSkillItems = supportsStructuredSkillInput && skillMentions.isNotEmpty()
            var effectiveCollaborationMode =
                if (supportsTurnCollaborationMode) collaborationMode else null
            var didDowngradePlanModeForRuntime = false

            // Resume the thread on the runtime before turn/start (matches iOS ensureThreadResumed flow).
            // Without this, turn/start fails with "thread not found" after a bridge/runtime restart.
            try {
                ensureThreadResumed(threadId = tid)
            } catch (e: Exception) {
                Log.w(TAG, "thread/resume failed before turn/start, proceeding anyway: ${e.message}")
            }

            while (true) {
                try {
                    val params = mutableMapOf<String, JsonValue>(
                        "threadId" to JsonValue.string(tid)
                    )

                    val inputItems = mutableListOf<JsonValue>()
                    attachments.forEach { att ->
                        val payloadDataUrl = att.payloadDataURL?.trim().orEmpty()
                        if (payloadDataUrl.isNotEmpty()) {
                            inputItems += JsonValue.obj(
                                "type" to JsonValue.string("image"),
                                "url" to JsonValue.string(payloadDataUrl)
                            )
                        }
                    }
                    if (trimmedText.isNotEmpty()) {
                        inputItems += JsonValue.obj(
                            "type" to JsonValue.string("text"),
                            "text" to JsonValue.string(trimmedText)
                        )
                    }
                    if (includeStructuredSkillItems) {
                        skillMentions.forEach { mention ->
                            val normalizedSkillId = mention.id.trim()
                            if (normalizedSkillId.isEmpty()) {
                                return@forEach
                            }

                            val skillPayload = mutableMapOf<String, JsonValue>(
                                "type" to JsonValue.string("skill"),
                                "id" to JsonValue.string(normalizedSkillId)
                            )
                            mention.name?.trim()?.takeIf { it.isNotEmpty() }?.let {
                                skillPayload["name"] = JsonValue.string(it)
                            }
                            mention.path?.trim()?.takeIf { it.isNotEmpty() }?.let {
                                skillPayload["path"] = JsonValue.string(it)
                            }
                            inputItems += JsonValue.ObjectValue(skillPayload)
                        }
                    }
                    params["input"] = JsonValue.ArrayValue(inputItems)

                    if (attachments.isNotEmpty()) {
                        params["images"] = JsonValue.array(*attachments.map { att ->
                            JsonValue.obj(
                                "url" to JsonValue.string(att.payloadDataURL ?: ""),
                                "thumbnail" to JsonValue.string(att.thumbnailBase64JPEG ?: "")
                            )
                        }.toTypedArray())
                    }

                    runtimeModelIdentifierForTurn()?.let { params["model"] = JsonValue.string(it) }
                    selectedReasoningEffortForSelectedModel()?.let { params["effort"] = JsonValue.string(it) }
                    _selectedServiceTier.value?.let { params["serviceTier"] = JsonValue.string(it.name.lowercase()) }
                    effectiveCollaborationMode?.let {
                        params["collaborationMode"] = buildCollaborationModePayload(it)
                    }

                    requireSuccessfulResponse(
                        sendTurnStartRequest(params),
                        "turn/start"
                    )

                    updateMessageDeliveryState(tid, optimisticMsg.id, CodexMessageDeliveryState.CONFIRMED)
                    markThreadRunning(tid)

                    if (didDowngradePlanModeForRuntime) {
                        appendMessage(
                            tid,
                            CodexMessage(
                                threadId = tid,
                                role = CodexMessageRole.SYSTEM,
                                kind = CodexMessageKind.CHAT,
                                text = "Plan mode is not supported by this runtime. Sent as a normal turn instead.",
                                orderIndex = CodexMessageOrderCounter.next(),
                                deliveryState = CodexMessageDeliveryState.CONFIRMED
                            )
                        )
                    }

                    return@launch
                } catch (e: Exception) {
                    if (includeStructuredSkillItems && shouldRetryTurnStartWithoutSkillItems(e)) {
                        Log.w(TAG, "Retrying turn/start without structured skill items: ${e.message}")
                        supportsStructuredSkillInput = false
                        includeStructuredSkillItems = false
                        continue
                    }

                    if (effectiveCollaborationMode != null
                        && shouldRetryTurnStartWithoutCollaborationMode(e)
                    ) {
                        Log.w(TAG, "Retrying turn/start without collaborationMode: ${e.message}")
                        supportsTurnCollaborationMode = false
                        effectiveCollaborationMode = null
                        didDowngradePlanModeForRuntime = true
                        continue
                    }

                    Log.e(TAG, "Send message failed: ${e.message}")
                    updateMessageDeliveryState(tid, optimisticMsg.id, CodexMessageDeliveryState.FAILED)
                    clearRunningState(tid)
                    appendMessage(
                        tid,
                        CodexMessage(
                            threadId = tid,
                            role = CodexMessageRole.SYSTEM,
                            kind = CodexMessageKind.CHAT,
                            text = "Send error: ${e.message ?: "Unknown error"}",
                            orderIndex = CodexMessageOrderCounter.next(),
                            deliveryState = CodexMessageDeliveryState.CONFIRMED
                        )
                    )
                    return@launch
                }
            }
        }
    }

    suspend fun steerTurn(
        text: String,
        threadId: String,
        attachments: List<CodexImageAttachment> = emptyList(),
        skillMentions: List<CodexTurnSkillMention> = emptyList(),
        collaborationMode: CodexCollaborationModeKind? = null
    ) {
        val normalizedThreadId = normalizedIdentifier(threadId)
            ?: throw IllegalArgumentException("A thread id is required.")
        if (thread(normalizedThreadId)?.syncState == CodexThreadSyncState.ARCHIVED_LOCAL
            || locallyArchivedThreadIDs().contains(normalizedThreadId)
        ) {
            throw IllegalStateException("Archived chats cannot be steered.")
        }

        val trimmedText = text.trim()
        if (trimmedText.isBlank() && attachments.isEmpty()) {
            throw IllegalArgumentException("A queued draft needs text or attachments.")
        }

        val optimisticMsg = CodexMessage(
            threadId = normalizedThreadId,
            role = CodexMessageRole.USER,
            kind = CodexMessageKind.CHAT,
            text = trimmedText,
            orderIndex = CodexMessageOrderCounter.next(),
            deliveryState = CodexMessageDeliveryState.PENDING,
            attachments = attachments
        )
        appendMessage(normalizedThreadId, optimisticMsg)

        var resolvedExpectedTurnId = normalizedIdentifier(activeTurnIdByThread[normalizedThreadId])
            ?: resolveInFlightTurnId(normalizedThreadId)
            ?: run {
                updateMessageDeliveryState(
                    normalizedThreadId,
                    optimisticMsg.id,
                    CodexMessageDeliveryState.FAILED
                )
                throw IllegalStateException("No active turn is available to steer.")
            }

        var includeStructuredSkillItems = supportsStructuredSkillInput && skillMentions.isNotEmpty()
        var effectiveCollaborationMode =
            if (supportsTurnCollaborationMode) collaborationMode else null
        var didRetryWithRefreshedTurnId = false

        while (true) {
            try {
                val inputItems = mutableListOf<JsonValue>()
                attachments.forEach { att ->
                    val payloadDataUrl = att.payloadDataURL?.trim().orEmpty()
                    if (payloadDataUrl.isNotEmpty()) {
                        inputItems += JsonValue.obj(
                            "type" to JsonValue.string("image"),
                            "url" to JsonValue.string(payloadDataUrl)
                        )
                    }
                }
                if (trimmedText.isNotEmpty()) {
                    inputItems += JsonValue.obj(
                        "type" to JsonValue.string("text"),
                        "text" to JsonValue.string(trimmedText)
                    )
                }
                if (includeStructuredSkillItems) {
                    skillMentions.forEach { mention ->
                        val normalizedSkillId = mention.id.trim()
                        if (normalizedSkillId.isEmpty()) {
                            return@forEach
                        }

                        val skillPayload = mutableMapOf<String, JsonValue>(
                            "type" to JsonValue.string("skill"),
                            "id" to JsonValue.string(normalizedSkillId)
                        )
                        mention.name?.trim()?.takeIf { it.isNotEmpty() }?.let {
                            skillPayload["name"] = JsonValue.string(it)
                        }
                        mention.path?.trim()?.takeIf { it.isNotEmpty() }?.let {
                            skillPayload["path"] = JsonValue.string(it)
                        }
                        inputItems += JsonValue.ObjectValue(skillPayload)
                    }
                }

                val params = mutableMapOf<String, JsonValue>(
                    "threadId" to JsonValue.string(normalizedThreadId),
                    "expectedTurnId" to JsonValue.string(resolvedExpectedTurnId),
                    "input" to JsonValue.ArrayValue(inputItems)
                )
                effectiveCollaborationMode?.let {
                    params["collaborationMode"] = buildCollaborationModePayload(it)
                }

                requireSuccessfulResponse(
                    sendRequestWithSandboxFallback("turn/steer", params),
                    "turn/steer"
                )

                updateMessageDeliveryState(
                    normalizedThreadId,
                    optimisticMsg.id,
                    CodexMessageDeliveryState.CONFIRMED
                )
                activeTurnIdByThread[normalizedThreadId] = resolvedExpectedTurnId
                markThreadRunning(normalizedThreadId)
                return
            } catch (error: Exception) {
                if (includeStructuredSkillItems && shouldRetryTurnStartWithoutSkillItems(error)) {
                    Log.w(TAG, "Retrying turn/steer without structured skill items: ${error.message}")
                    supportsStructuredSkillInput = false
                    includeStructuredSkillItems = false
                    continue
                }

                if (effectiveCollaborationMode != null
                    && shouldRetryTurnStartWithoutCollaborationMode(error)
                ) {
                    Log.w(TAG, "Retrying turn/steer without collaborationMode: ${error.message}")
                    supportsTurnCollaborationMode = false
                    effectiveCollaborationMode = null
                    continue
                }

                if (!didRetryWithRefreshedTurnId && shouldRetrySteerWithRefreshedTurnId(error)) {
                    val refreshedTurnId = resolveInFlightTurnId(normalizedThreadId)
                    if (refreshedTurnId != null && refreshedTurnId != resolvedExpectedTurnId) {
                        didRetryWithRefreshedTurnId = true
                        resolvedExpectedTurnId = refreshedTurnId
                        activeTurnIdByThread[normalizedThreadId] = refreshedTurnId
                        continue
                    }
                }

                updateMessageDeliveryState(
                    normalizedThreadId,
                    optimisticMsg.id,
                    CodexMessageDeliveryState.FAILED
                )
                throw error
            }
        }
    }

    fun stopTurn(threadId: String? = null) {
        val tid = normalizedIdentifier(threadId ?: _activeThreadId.value) ?: return

        scope.launch {
            try {
                interruptTurn(threadId = tid)
            } catch (e: Exception) {
                Log.e(TAG, "Stop turn failed: ${e.message}")
            }
        }
    }

    private suspend fun interruptTurn(threadId: String) {
        var resolvedTurnId = normalizedIdentifier(activeTurnIdByThread[threadId])
            ?: resolveInFlightTurnId(threadId)
            ?: throw IllegalArgumentException("turn/interrupt requires a non-empty turnId")

        try {
            sendInterruptRequest(
                turnId = resolvedTurnId,
                threadId = threadId,
                useSnakeCaseParams = false
            )
            return
        } catch (error: Exception) {
            var finalError: Exception = error

            if (shouldRetryInterruptWithSnakeCaseParams(error)) {
                try {
                    sendInterruptRequest(
                        turnId = resolvedTurnId,
                        threadId = threadId,
                        useSnakeCaseParams = true
                    )
                    return
                } catch (snakeCaseError: Exception) {
                    finalError = snakeCaseError
                }
            }

            if (shouldRetryInterruptWithRefreshedTurnId(finalError)) {
                val refreshedTurnId = resolveInFlightTurnId(threadId)
                if (refreshedTurnId != null && refreshedTurnId != resolvedTurnId) {
                    resolvedTurnId = refreshedTurnId
                    activeTurnIdByThread[threadId] = refreshedTurnId

                    try {
                        sendInterruptRequest(
                            turnId = refreshedTurnId,
                            threadId = threadId,
                            useSnakeCaseParams = false
                        )
                        return
                    } catch (refreshedError: Exception) {
                        finalError = refreshedError
                        if (shouldRetryInterruptWithSnakeCaseParams(refreshedError)) {
                            sendInterruptRequest(
                                turnId = refreshedTurnId,
                                threadId = threadId,
                                useSnakeCaseParams = true
                            )
                            return
                        }
                    }
                }
            }

            throw finalError
        }
    }

    private suspend fun sendInterruptRequest(
        turnId: String,
        threadId: String,
        useSnakeCaseParams: Boolean
    ) {
        val params = mutableMapOf<String, JsonValue>()
        params[if (useSnakeCaseParams) "turn_id" else "turnId"] = JsonValue.string(turnId)
        params[if (useSnakeCaseParams) "thread_id" else "threadId"] = JsonValue.string(threadId)
        requireSuccessfulResponse(
            messageTransport.sendRequest("turn/interrupt", JsonValue.ObjectValue(params)),
            "turn/interrupt"
        )
    }

    private suspend fun resolveInFlightTurnId(threadId: String): String? {
        repeat(3) { attempt ->
            val snapshot = readThreadTurnStateSnapshot(threadId)
            snapshot.interruptibleTurnId?.let { return it }
            if (snapshot.hasInterruptibleTurnWithoutId) {
                if (attempt < 2) {
                    delay(200)
                } else {
                    throw IllegalArgumentException(
                        "The active run has not published an interruptible turn ID yet. Please try again in a moment."
                    )
                }
            } else {
                return null
            }
        }
        return null
    }

    private suspend fun readThreadTurnStateSnapshot(threadId: String): InterruptibleTurnSnapshot {
        val response = try {
            requireSuccessfulResponse(
                messageTransport.sendRequest(
                    "thread/read",
                    JsonValue.obj(
                        "threadId" to JsonValue.string(threadId),
                        "includeTurns" to JsonValue.bool(true)
                    )
                ),
                "thread/read"
            )
        } catch (error: Exception) {
            if (!shouldRetryThreadReadTurnSnapshotWithSnakeCase(error)) {
                throw error
            }

            requireSuccessfulResponse(
                messageTransport.sendRequest(
                    "thread/read",
                    JsonValue.obj(
                        "thread_id" to JsonValue.string(threadId),
                        "include_turns" to JsonValue.bool(true)
                    )
                ),
                "thread/read"
            )
        }

        return decodeInterruptibleTurnSnapshot(response.result)
    }

    // --- Approval ---

    fun respondToApproval(approve: Boolean) {
        val approval = _pendingApproval.value ?: return
        _pendingApproval.value = null

        val decision = if (approve) "accept" else "reject"
        messageTransport.sendResponse(
            approval.requestId,
            JsonValue.obj("decision" to JsonValue.string(decision))
        )
    }

    fun respondToStructuredUserInput(
        requestID: JsonValue,
        answersByQuestionID: Map<String, List<String>>
    ) {
        scope.launch {
            try {
                messageTransport.sendResponse(
                    requestID,
                    buildStructuredUserInputResponse(answersByQuestionID)
                )
                removeStructuredUserInputPrompt(requestID, _activeThreadId.value)
            } catch (e: Exception) {
                Log.e(TAG, "Structured input response failed: ${e.message}")
            }
        }
    }

    fun setAccessMode(mode: CodexAccessMode) {
        _selectedAccessMode.value = mode
        secureStore.writeString(SecureStore.SELECTED_ACCESS_MODE, mode.name)
    }

    fun setSelectedModel(modelId: String?) {
        _selectedModel.value = modelId?.trim()?.takeIf { it.isNotEmpty() }
        normalizeRuntimeSelectionsAfterModelsUpdate()
    }

    fun setSelectedReasoningEffort(effort: String?) {
        _selectedReasoningEffort.value = effort?.trim()?.takeIf { it.isNotEmpty() }
        normalizeRuntimeSelectionsAfterModelsUpdate()
    }

    fun setSelectedServiceTier(serviceTier: CodexServiceTier?) {
        _selectedServiceTier.value = serviceTier
        persistRuntimeSelections()
    }

    // --- Notifications ---

    private fun handleBufferedReplayControl(
        normalizedMethod: String,
        payload: Map<String, JsonValue>
    ): Boolean {
        val isReset = normalizedMethod == "remodex/bufferedreplay/reset"
            || firstBoolValue(payload, "remodexBufferedReplayReset") == true
        val isGap = normalizedMethod == "remodex/bufferedreplay/gap"
            || firstBoolValue(payload, "remodexBufferedReplayGap") == true
        val isComplete = normalizedMethod == "remodex/bufferedreplay/completed"
            || firstBoolValue(payload, "remodexBufferedReplayComplete") == true

        if (isReset || isGap) {
            val cursor = if (isGap) {
                firstLongValue(payload, "lastDiscardedBridgeOutboundSeq")
            } else {
                firstLongValue(payload, "resetBridgeOutboundSeqTo")
            } ?: 0L
            secureTransport.resetReplayCursor(
                cursor = cursor.coerceAtMost(Int.MAX_VALUE.toLong()).toInt(),
                replayEpoch = firstStringValue(payload, "bridgeReplayEpoch")
            )
            threadHistoryHydrator.reset()
            bufferedReplayRefreshPending = true
            return true
        }

        if (isComplete) {
            if (bufferedReplayRefreshPending) {
                bufferedReplayRefreshPending = false
                val activeThreadId = _activeThreadId.value
                if (!activeThreadId.isNullOrBlank()) {
                    requestThreadHistoryLoad(activeThreadId, forceRefresh = true)
                }
            }
            return true
        }

        return false
    }

    private fun isReplayOnlyLifecycleOrDelta(method: String): Boolean {
        return method.contains("turn/started")
            || method.contains("turn_started")
            || method.contains("turn/completed")
            || method.contains("turn_completed")
            || method.contains("turn/failed")
            || method.contains("turn_failed")
            || method.contains("/delta")
            || method.contains("_delta")
            || method.contains("agent.delta")
    }

    private fun handleNotification(method: String, params: JsonValue?) {
        val p = params?.objectValue ?: emptyMap()
        val item = p["item"]?.objectValue
        val normalizedMethod = method.lowercase()
        if (handleBufferedReplayControl(normalizedMethod, p)) {
            return
        }
        val isReplayedEvent = firstBoolValue(p, "remodexReplayedEvent") == true
        if (isReplayedEvent && isReplayOnlyLifecycleOrDelta(normalizedMethod)) {
            return
        }
        val legacyEventType = if (normalizedMethod == "event") {
            firstStringValue(envelopeEventObject(p) ?: emptyMap(), "type")
                ?.trim()
                ?.lowercase()
                ?.replace('-', '_')
        } else {
            null
        }
        val threadId = extractNotificationThreadId(p)
        val turnId = extractNotificationTurnId(p)
        val itemId = firstStringValue(
            p,
            "itemId",
            "item_id",
            "call_id",
            "callId"
        )
            ?: firstStringValue(item ?: emptyMap(), "id", "itemId", "item_id", "call_id", "callId")
            ?: firstStringValue(
                envelopeEventObject(p) ?: emptyMap(),
                "itemId",
                "item_id",
                "call_id",
                "callId"
            )

        when {
            shouldHandleTurnDiffNotification(normalizedMethod, p) -> {
                val patch = extractTurnDiffPatch(p).orEmpty()
                if (threadId != null && turnId != null && patch.isNotBlank()) {
                    recordTurnDiffChangeSet(
                        threadId = threadId,
                        turnId = turnId,
                        patch = patch,
                        workingDirectory = thread(threadId)?.gitWorkingDirectory
                    )
                }
            }
            normalizedMethod == "thread/tokenusage/updated"
                || normalizedMethod == "thread/token_usage/updated"
                || legacyEventType == "token_count" -> {
                val usage = extractContextWindowUsageSnapshot(
                    payload = when {
                        legacyEventType == "token_count" -> envelopeEventObject(p) ?: p
                        else -> p["usage"]?.objectValue ?: envelopeEventObject(p)?.get("usage")?.objectValue ?: p
                    },
                    isTokenCountPayload = legacyEventType == "token_count"
                )
                val activeThreadId = normalizedIdentifier(_activeThreadId.value)
                val usageThreadId = threadId
                    ?: activeTurnIdByThread.entries.firstOrNull { (_, candidateTurnId) ->
                        normalizedIdentifier(candidateTurnId) == normalizedIdentifier(turnId)
                    }?.key
                if (usage != null && (activeThreadId == null || usageThreadId == null || usageThreadId == activeThreadId)) {
                    _contextWindowUsage.value = usage
                }
            }
            normalizedMethod == "thread/status/changed"
                || normalizedMethod == "thread/status"
                || normalizedMethod == "thread_status_changed" -> {
                if (threadId != null) {
                    handleThreadStatusChangedNotification(
                        threadId = threadId,
                        turnId = turnId,
                        payload = p
                    )
                }
            }
            !isReplayedEvent && (method.contains("turn/started") || method.contains("turn_started")) -> {
                if (threadId != null) {
                    markThreadRunning(threadId)
                    if (turnId != null) activeTurnIdByThread[threadId] = turnId
                }
            }
            !isReplayedEvent && (method.contains("turn/completed") || method.contains("turn_completed")) -> {
                if (threadId != null) {
                    clearRunningState(threadId)
                    _readyThreadIDs.value = _readyThreadIDs.value + threadId
                    activeTurnIdByThread.remove(threadId)
                    assistantHandler.clearStreamingState()
                    notifyRunCompletionIfNeeded(
                        threadId = threadId,
                        turnId = turnId,
                        result = CodexRunCompletionResult.COMPLETED
                    )
                }
            }
            !isReplayedEvent && (method.contains("turn/failed") || method.contains("turn_failed")) -> {
                if (threadId != null) {
                    clearRunningState(threadId)
                    _failedThreadIDs.value = _failedThreadIDs.value + threadId
                    activeTurnIdByThread.remove(threadId)
                    notifyRunCompletionIfNeeded(
                        threadId = threadId,
                        turnId = turnId,
                        result = CodexRunCompletionResult.FAILED
                    )
                }
            }
            method.contains("item/agentMessage/delta") || method.contains("item_agentMessage_delta") ||
            method.contains("item/agentmessage/delta") || method.contains("item_agentmessage_delta") ||
            method.contains("item/agent/delta") || method.contains("item_agent_delta") ||
            method.contains("agent.delta") -> {
                val text = p["delta"]?.stringValue
                    ?: p["text"]?.stringValue ?: ""
                if (threadId != null && text.isNotEmpty()) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    assistantHandler.appendAgentDelta(threadId, turnId, itemId, text, messages)
                    setThreadMessages(threadId, messages)
                }
            }
            normalizedMethod == "exec_command_begin"
                || normalizedMethod == "exec_command_output_delta"
                || normalizedMethod == "exec_command_end" -> {
                if (threadId != null) {
                    val delta = extractIncomingDeltaText(p, item)
                    val payload = payloadWithDelta(p, delta)
                    val isCompleted = normalizedMethod == "exec_command_end"
                    val messages = getThreadMessages(threadId).toMutableList()
                    upsertSystemMessage(
                        threadId = threadId,
                        turnId = turnId,
                        itemId = itemId,
                        kind = CodexMessageKind.COMMAND_EXECUTION,
                        text = decodeCommandExecutionStatusText(payload, isCompleted = isCompleted),
                        isStreaming = !isCompleted,
                        messages = messages,
                        commandDetails = extractCommandExecutionDetails(payload)
                    )
                    setThreadMessages(threadId, messages)
                }
            }
            normalizedMethod == "background_event"
                || normalizedMethod == "read"
                || normalizedMethod == "search"
                || normalizedMethod == "list_files"
                || legacyEventType in setOf("background_event", "read", "search", "list_files") -> {
                val resolvedEventType = legacyEventType ?: normalizedMethod
                val activityText = legacyToolActivityLine(
                    eventType = resolvedEventType,
                    payload = envelopeEventObject(p) ?: p
                )
                if (threadId != null && activityText != null) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    upsertSystemMessage(
                        threadId = threadId,
                        turnId = turnId,
                        itemId = itemId,
                        kind = CodexMessageKind.TOOL_ACTIVITY,
                        text = activityText,
                        isStreaming = true,
                        messages = messages,
                        appendDuringStreaming = true
                    )
                    setThreadMessages(threadId, messages)
                }
            }
            normalizedMethod == "patch_apply_begin"
                || normalizedMethod == "patch_apply_end"
                || legacyEventType == "patch_apply_begin"
                || legacyEventType == "patch_apply_end" -> {
                if (threadId != null) {
                    val resolvedEventType = legacyEventType ?: normalizedMethod
                    val payload = envelopeEventObject(p) ?: p
                    val status = firstStringValue(payload, "status", "phase", "state")
                        ?: when (resolvedEventType) {
                            "patch_apply_end" -> {
                                if (firstBoolValue(payload, "success") == false) "failed" else "completed"
                            }
                            else -> "running"
                        }
                    val fileChangePayload = buildMap<String, JsonValue> {
                        put("status", JsonValue.string(status))
                        payload["changes"]?.let { put("changes", it) }
                        payload["diff"]?.let { put("diff", it) }
                        payload["unified_diff"]?.let { put("unified_diff", it) }
                        payload["patch"]?.let { put("patch", it) }
                    }
                    historyDecoder.decodeToolCallMessage(fileChangePayload)?.let { (kind, text) ->
                        val messages = getThreadMessages(threadId).toMutableList()
                        upsertSystemMessage(
                            threadId = threadId,
                            turnId = turnId,
                            itemId = itemId,
                            kind = kind,
                            text = text,
                            isStreaming = resolvedEventType == "patch_apply_begin",
                            messages = messages,
                            appendDuringStreaming = resolvedEventType == "patch_apply_begin"
                        )
                        setThreadMessages(threadId, messages)
                    }
                }
            }
            normalizedMethod.contains("item/filechange/outputdelta")
                || normalizedMethod.contains("item/filechange/output_delta")
                || normalizedMethod.contains("item/file_change/outputdelta")
                || normalizedMethod.contains("item/file_change/output_delta") -> {
                val text = extractIncomingDeltaText(p, item)
                    .ifBlank { item?.let(::extractFileChangeText).orEmpty() }
                if (threadId != null && text.isNotBlank()) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    upsertSystemMessage(
                        threadId = threadId,
                        turnId = turnId,
                        itemId = itemId,
                        kind = CodexMessageKind.FILE_CHANGE,
                        text = text,
                        isStreaming = true,
                        messages = messages,
                        appendDuringStreaming = true
                    )
                    setThreadMessages(threadId, messages)
                }
            }
            normalizedMethod.contains("item/toolcall/outputdelta")
                || normalizedMethod.contains("item/toolcall/output_delta")
                || normalizedMethod.contains("item/tool_call/outputdelta")
                || normalizedMethod.contains("item/tool_call/output_delta") -> {
                if (threadId != null) {
                    val delta = extractIncomingDeltaText(p, item)
                    val payload = payloadWithDelta(item ?: p, delta)
                    val messages = getThreadMessages(threadId).toMutableList()
                    historyDecoder.decodeToolCallMessage(payload)?.let { (kind, text) ->
                        upsertSystemMessage(
                            threadId = threadId,
                            turnId = turnId,
                            itemId = itemId,
                            kind = kind,
                            text = text,
                            isStreaming = true,
                            messages = messages,
                            appendDuringStreaming = kind == CodexMessageKind.TOOL_ACTIVITY
                        )
                    }
                    setThreadMessages(threadId, messages)
                }
            }
            normalizedMethod.contains("item/commandexecution/outputdelta")
                || normalizedMethod.contains("item/commandexecution/output_delta")
                || normalizedMethod.contains("item/command_execution/outputdelta")
                || normalizedMethod.contains("item/command_execution/output_delta")
                || normalizedMethod.contains("item/commandexecution/terminalinteraction")
                || normalizedMethod.contains("item/command_execution/terminalinteraction") -> {
                if (threadId != null) {
                    val delta = extractIncomingDeltaText(p, item)
                    val payload = payloadWithDelta(item ?: p, delta)
                    val messages = getThreadMessages(threadId).toMutableList()
                    upsertSystemMessage(
                        threadId = threadId,
                        turnId = turnId,
                        itemId = itemId,
                        kind = CodexMessageKind.COMMAND_EXECUTION,
                        text = decodeCommandExecutionStatusText(payload, isCompleted = false),
                        isStreaming = true,
                        messages = messages,
                        commandDetails = extractCommandExecutionDetails(payload)
                    )
                    setThreadMessages(threadId, messages)
                }
            }
            method.contains("item/completed") || method.contains("item_completed") -> {
                if (threadId != null && item != null) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    when (val itemType = extractItemType(item)) {
                        "agentmessage", "assistantmessage", "message" -> {
                            assistantHandler.completeAgentMessage(
                                threadId,
                                turnId,
                                itemId,
                                extractItemText(item),
                                messages
                            )?.let(::noteAssistantMessage)
                        }
                        "commandexecution", "command_execution" -> {
                            val text = extractCommandExecutionText(item)
                            if (text.isNotBlank()) {
                                upsertSystemMessage(
                                    threadId = threadId,
                                    turnId = turnId,
                                    itemId = itemId,
                                    kind = CodexMessageKind.COMMAND_EXECUTION,
                                    text = text,
                                    isStreaming = false,
                                    messages = messages,
                                    commandDetails = extractCommandExecutionDetails(item)
                                )
                            }
                        }
                        "filechange", "file_change", "diff" -> {
                            val text = extractFileChangeText(item)
                            if (text.isNotBlank()) {
                                upsertSystemMessage(
                                    threadId = threadId,
                                    turnId = turnId,
                                    itemId = itemId,
                                    kind = CodexMessageKind.FILE_CHANGE,
                                    text = text,
                                    isStreaming = false,
                                    messages = messages
                                )
                            }
                        }
                        "toolcall", "tool_call" -> {
                            historyDecoder.decodeToolCallMessage(item)?.let { (kind, text) ->
                                upsertSystemMessage(
                                    threadId = threadId,
                                    turnId = turnId,
                                    itemId = itemId,
                                    kind = kind,
                                    text = text,
                                    isStreaming = false,
                                    messages = messages
                                )
                            }
                        }
                        "reasoning" -> {
                            val text = extractItemText(item)
                            if (text.isNotBlank()) {
                                upsertThinkingMessage(
                                    threadId = threadId,
                                    turnId = turnId,
                                    itemId = itemId,
                                    text = text,
                                    isStreaming = false,
                                    messages = messages
                                )
                            }
                        }
                        "plan" -> {
                            upsertPlanMessage(
                                threadId = threadId,
                                turnId = turnId,
                                itemId = itemId,
                                text = historyDecoder.decodePlanItemText(item),
                                planState = historyDecoder.decodePlanState(item),
                                isStreaming = false,
                                messages = messages
                            )
                        }
                        "enteredreviewmode", "entered_review_mode" -> {
                            val reviewLabel = item["review"]?.stringValue
                                ?: item["label"]?.stringValue
                                ?: item["title"]?.stringValue
                                ?: "changes"
                            upsertSystemMessage(
                                threadId = threadId,
                                turnId = turnId,
                                itemId = itemId,
                                kind = CodexMessageKind.CHAT,
                                text = "Reviewing $reviewLabel...",
                                isStreaming = false,
                                messages = messages
                            )
                        }
                        "exitedreviewmode", "exited_review_mode" -> {
                            val reviewText = item["review"]?.stringValue
                                ?: item["text"]?.stringValue
                                ?: item["message"]?.stringValue
                                ?: extractItemText(item)
                            if (reviewText.isNotBlank()) {
                                messages.add(
                                    CodexMessage(
                                        threadId = threadId,
                                        role = CodexMessageRole.ASSISTANT,
                                        kind = CodexMessageKind.CHAT,
                                        text = reviewText,
                                        createdAt = System.currentTimeMillis(),
                                        turnId = turnId,
                                        itemId = itemId,
                                        orderIndex = CodexMessageOrderCounter.next(),
                                        deliveryState = CodexMessageDeliveryState.CONFIRMED
                                    )
                                )
                            }
                        }
                        else -> {
                            if (isSubagentItemType(itemType)) {
                                historyDecoder.decodeSubagentAction(item, itemType)?.let { action ->
                                    upsertSubagentActionMessage(
                                        threadId = threadId,
                                        turnId = turnId,
                                        itemId = itemId,
                                        action = action,
                                        isStreaming = false,
                                        messages = messages
                                    )
                                }
                            }
                        }
                    }
                    extractPatchFromJsonValue(JsonValue.ObjectValue(item))
                        ?.takeIf { it.isNotBlank() && turnId != null }
                        ?.let { patch ->
                            recordFallbackFileChangePatch(
                                threadId = threadId,
                                turnId = turnId!!,
                                patch = patch,
                                workingDirectory = thread(threadId)?.gitWorkingDirectory
                            )
                        }
                    setThreadMessages(threadId, messages)
                }
            }
            method.contains("item/agent/completed") || method.contains("item_agent_completed") ||
            method.contains("agent.completed") -> {
                val text = p["text"]?.stringValue ?: p["content"]?.stringValue
                if (threadId != null) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    assistantHandler.completeAgentMessage(threadId, turnId, itemId, text, messages)
                        ?.let(::noteAssistantMessage)
                    setThreadMessages(threadId, messages)
                }
            }
            method.contains("item/agent/started") || method.contains("item_agent_started") -> {
                // Agent item started — no action needed beyond running state
            }
            method.contains("item/fileChange") || method.contains("file_change") -> {
                val text = p["path"]?.stringValue ?: p["summary"]?.stringValue ?: ""
                if (threadId != null && text.isNotEmpty()) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    upsertSystemMessage(
                        threadId = threadId,
                        turnId = turnId,
                        itemId = itemId,
                        kind = CodexMessageKind.FILE_CHANGE,
                        text = text,
                        isStreaming = true,
                        messages = messages
                    )
                    setThreadMessages(threadId, messages)
                }
            }
            method.contains("item/commandExecution") || method.contains("command_execution") -> {
                val command = p["command"]?.stringValue ?: p["call"]?.stringValue ?: ""
                if (threadId != null) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    val details = CommandExecutionDetails(
                        fullCommand = command,
                        cwd = p["cwd"]?.stringValue,
                        exitCode = p["exitCode"]?.intValue?.toInt(),
                        outputTail = p["output"]?.stringValue ?: ""
                    )
                    upsertSystemMessage(
                        threadId = threadId,
                        turnId = turnId,
                        itemId = itemId,
                        kind = CodexMessageKind.COMMAND_EXECUTION,
                        text = decodeCommandExecutionStatusText(p, isCompleted = false),
                        isStreaming = true,
                        messages = messages,
                        commandDetails = details
                    )
                    setThreadMessages(threadId, messages)
                }
            }
            method.contains("item/reasoning") || method.contains("reasoning") -> {
                val text = p["text"]?.stringValue ?: p["reasoning"]?.stringValue ?: ""
                if (threadId != null && text.isNotEmpty()) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    upsertThinkingMessage(
                        threadId = threadId,
                        turnId = turnId,
                        itemId = itemId,
                        text = text,
                        isStreaming = true,
                        messages = messages
                    )
                    setThreadMessages(threadId, messages)
                }
            }
            method.contains("turn/plan/updated") || method.contains("turn_plan_updated") -> {
                if (threadId != null) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    upsertPlanMessage(
                        threadId = threadId,
                        turnId = turnId,
                        itemId = itemId,
                        text = p["explanation"]?.stringValue ?: p["summary"]?.stringValue ?: "Planning...",
                        planState = CodexPlanState(
                            explanation = firstStringValue(p, "explanation", "summary"),
                            steps = historyDecoder.decodePlanState(p)?.steps ?: emptyList()
                        ),
                        isStreaming = true,
                        messages = messages
                    )
                    setThreadMessages(threadId, messages)
                }
            }
            method.contains("item/plan/delta") || method.contains("item_plan_delta") -> {
                val text = p["delta"]?.stringValue ?: p["text"]?.stringValue ?: ""
                if (threadId != null) {
                    val messages = getThreadMessages(threadId).toMutableList()
                    upsertPlanMessage(
                        threadId = threadId,
                        turnId = turnId,
                        itemId = itemId,
                        text = text,
                        planState = null,
                        isStreaming = true,
                        messages = messages
                    )
                    setThreadMessages(threadId, messages)
                }
            }
            method.contains("serverRequest/resolved") || method.contains("serverrequest/resolved") ||
            method.contains("server_request_resolved") -> {
                firstJsonValue(p, "requestId", "request_id")?.let { requestId ->
                    removeStructuredUserInputPrompt(
                        requestID = requestId,
                        threadIdHint = firstStringValue(p, "threadId", "thread_id")
                    )
                }
            }
            method.contains("thread/list/update") || method.contains("thread_list_update") -> {
                scope.launch {
                    syncThreadList()
                    routePendingNotificationOpenIfPossible(refreshIfNeeded = false)
                }
            }
            method.contains("models/available") -> {
                val models = p["models"]?.arrayValue?.mapNotNull { m ->
                    try { json.decodeFromString(CodexModelOption.serializer(),
                        json.encodeToString(JsonValue.serializer(), m)) }
                    catch (_: Exception) { null }
                } ?: emptyList()
                _availableModels.value = models
                normalizeRuntimeSelectionsAfterModelsUpdate()
            }
        }
    }

    private fun selectedModelOption(models: List<CodexModelOption> = _availableModels.value): CodexModelOption? {
        if (models.isEmpty()) {
            return null
        }
        val selectedModelId = _selectedModel.value
        if (!selectedModelId.isNullOrBlank()) {
            models.firstOrNull { it.id == selectedModelId || it.model == selectedModelId }?.let { return it }
        }
        return null
    }

    private fun fallbackModel(models: List<CodexModelOption> = _availableModels.value): CodexModelOption? =
        models.firstOrNull { it.isDefault } ?: models.firstOrNull()

    private fun runtimeModelIdentifierForTurn(): String? =
        (selectedModelOption() ?: fallbackModel())?.model

    private fun buildCollaborationModePayload(mode: CodexCollaborationModeKind): JsonValue {
        val resolvedModel = runtimeModelIdentifierForTurn()
            ?: selectedModelOption()?.model
            ?: fallbackModel()?.model
            ?: _selectedModel.value
        val normalizedModel = resolvedModel
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?: throw IllegalStateException("Plan mode requires an available model before starting a plan turn.")

        return JsonValue.obj(
            "mode" to JsonValue.string(mode.wireValue),
            "settings" to JsonValue.obj(
                "model" to JsonValue.string(normalizedModel),
                "reasoning_effort" to (selectedReasoningEffortForSelectedModel()?.let(JsonValue::string)
                    ?: JsonValue.NullValue),
                "developer_instructions" to JsonValue.NullValue
            )
        )
    }

    private fun decodeFuzzyFileMatches(result: JsonValue?): List<CodexFuzzyFileMatch>? {
        val resultObject = result?.objectValue ?: return null
        val filesValue = resultObject["files"] ?: return null
        return try {
            json.decodeFromString(
                ListSerializer(CodexFuzzyFileMatch.serializer()),
                json.encodeToString(JsonValue.serializer(), filesValue)
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun decodeSkillMetadata(result: JsonValue?): List<CodexSkillMetadata>? {
        val resultObject = result?.objectValue ?: return null
        val collectedSkills = mutableListOf<CodexSkillMetadata>()
        var hasSkillContainer = false

        resultObject["data"]?.arrayValue?.let { dataItems ->
            hasSkillContainer = true
            dataItems.forEach { item ->
                val itemObject = item.objectValue ?: return@forEach
                itemObject["skills"]?.let { skillsValue ->
                    decodeSkillMetadataList(skillsValue)?.let(collectedSkills::addAll)
                }
            }

            if (collectedSkills.isEmpty()) {
                decodeSkillMetadataList(JsonValue.ArrayValue(dataItems))?.let(collectedSkills::addAll)
            }
        }

        resultObject["skills"]?.let { skillsValue ->
            hasSkillContainer = true
            if (collectedSkills.isEmpty()) {
                decodeSkillMetadataList(skillsValue)?.let(collectedSkills::addAll)
            }
        }

        return if (hasSkillContainer) collectedSkills else null
    }

    private fun decodeSkillMetadataList(value: JsonValue): List<CodexSkillMetadata>? {
        return try {
            json.decodeFromString(
                ListSerializer(CodexSkillMetadata.serializer()),
                json.encodeToString(JsonValue.serializer(), value)
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun normalizeFuzzyFilePath(path: String, root: String): String {
        val trimmedPath = path.trim()
        if (trimmedPath.isEmpty()) {
            return path
        }

        val normalizedRoot = normalizedFuzzyRootPath(root)
        if (normalizedRoot.isEmpty()) {
            return trimmedPath
        }

        if (normalizedRoot == "/") {
            return if (trimmedPath.startsWith("/")) trimmedPath.drop(1) else trimmedPath
        }

        val rootPrefix = if (normalizedRoot.endsWith("/")) normalizedRoot else "$normalizedRoot/"
        return if (trimmedPath.startsWith(rootPrefix)) {
            trimmedPath.removePrefix(rootPrefix)
        } else {
            trimmedPath
        }
    }

    private fun normalizedFuzzyRootPath(root: String): String =
        root.trim().removeSuffix("/")

    fun supportedReasoningEffortsForSelectedModel(): List<String> =
        (selectedModelOption() ?: fallbackModel())?.supportedReasoningEfforts.orEmpty()

    fun selectedReasoningEffortForSelectedModel(): String? {
        val resolvedModel = selectedModelOption() ?: fallbackModel() ?: return null
        val supported = resolvedModel.supportedReasoningEfforts.toSet()
        if (supported.isEmpty()) {
            return null
        }
        _selectedReasoningEffort.value?.let { selected ->
            if (supported.contains(selected)) {
                return selected
            }
        }
        resolvedModel.defaultReasoningEffort?.let { defaultEffort ->
            if (supported.contains(defaultEffort)) {
                return defaultEffort
            }
        }
        if (supported.contains("medium")) {
            return "medium"
        }
        return resolvedModel.supportedReasoningEfforts.firstOrNull()
    }

    private fun normalizeRuntimeSelectionsAfterModelsUpdate() {
        val models = _availableModels.value
        if (models.isEmpty()) {
            persistRuntimeSelections()
            return
        }

        val resolvedModel = selectedModelOption(models) ?: fallbackModel(models)
        _selectedModel.value = resolvedModel?.id

        val supported = resolvedModel?.supportedReasoningEfforts?.toSet().orEmpty()
        _selectedReasoningEffort.value = when {
            supported.isEmpty() -> null
            _selectedReasoningEffort.value != null && supported.contains(_selectedReasoningEffort.value) ->
                _selectedReasoningEffort.value
            resolvedModel?.defaultReasoningEffort != null && supported.contains(resolvedModel.defaultReasoningEffort) ->
                resolvedModel.defaultReasoningEffort
            supported.contains("medium") -> "medium"
            else -> resolvedModel?.supportedReasoningEfforts?.firstOrNull()
        }

        persistRuntimeSelections()
    }

    private fun persistRuntimeSelections() {
        _selectedModel.value?.let {
            secureStore.writeString(SecureStore.SELECTED_MODEL_ID, it)
        } ?: secureStore.deleteValue(SecureStore.SELECTED_MODEL_ID)

        _selectedReasoningEffort.value?.let {
            secureStore.writeString(SecureStore.SELECTED_REASONING_EFFORT, it)
        } ?: secureStore.deleteValue(SecureStore.SELECTED_REASONING_EFFORT)

        _selectedServiceTier.value?.let {
            secureStore.writeString(SecureStore.SELECTED_SERVICE_TIER, it.name)
        } ?: secureStore.deleteValue(SecureStore.SELECTED_SERVICE_TIER)
    }

    private suspend fun sendTurnStartRequest(baseParams: MutableMap<String, JsonValue>): RpcMessage {
        return sendRequestWithSandboxFallback("turn/start", baseParams)
    }

    private suspend fun sendRequestWithSandboxFallback(
        method: String,
        baseParams: MutableMap<String, JsonValue>
    ): RpcMessage {
        val sandboxPolicyParams = baseParams.toMutableMap().apply {
            this["sandboxPolicy"] = runtimeSandboxPolicyObject(_selectedAccessMode.value)
        }

        try {
            return sendRequestWithApprovalPolicyFallback(method, sandboxPolicyParams, "sandboxPolicy")
        } catch (error: Exception) {
            if (!shouldFallbackFromSandboxPolicy(error)) {
                throw error
            }
        }

        val legacySandboxParams = baseParams.toMutableMap().apply {
            this["sandbox"] = JsonValue.string(_selectedAccessMode.value.sandboxLegacyValue)
        }

        try {
            return sendRequestWithApprovalPolicyFallback(method, legacySandboxParams, "sandbox")
        } catch (error: Exception) {
            if (!shouldFallbackFromSandboxPolicy(error)) {
                throw error
            }
        }

        return sendRequestWithApprovalPolicyFallback(method, baseParams.toMutableMap(), "minimal")
    }

    private suspend fun sendRequestWithApprovalPolicyFallback(
        method: String,
        baseParams: MutableMap<String, JsonValue>,
        context: String
    ): RpcMessage {
        var lastError: Exception? = null
        val policies = _selectedAccessMode.value.approvalPolicyCandidates

        policies.forEachIndexed { index, policy ->
            val params = baseParams.toMutableMap().apply {
                this["approvalPolicy"] = JsonValue.string(policy)
            }

            try {
                return requireSuccessfulResponse(
                    messageTransport.sendRequest(method, JsonValue.ObjectValue(params)),
                    method
                )
            } catch (error: Exception) {
                lastError = error
                val hasMorePolicies = index < policies.lastIndex
                if (hasMorePolicies && shouldRetryWithApprovalPolicyFallback(error)) {
                    Log.w(TAG, "Retrying $method after $context approvalPolicy failure: ${error.message}")
                    return@forEachIndexed
                }
                throw error
            }
        }

        throw lastError ?: IllegalStateException("RPC $method failed with unknown approval policy error")
    }

    private fun runtimeSandboxPolicyObject(accessMode: CodexAccessMode): JsonValue {
        return when (accessMode) {
            CodexAccessMode.ON_REQUEST -> JsonValue.obj(
                "type" to JsonValue.string("workspaceWrite"),
                "networkAccess" to JsonValue.bool(true)
            )
            CodexAccessMode.FULL_ACCESS -> JsonValue.obj(
                "type" to JsonValue.string("dangerFullAccess")
            )
        }
    }

    private fun shouldFallbackFromSandboxPolicy(error: Exception): Boolean {
        val message = error.message.orEmpty().lowercase()
        return message.contains("sandboxpolicy")
            || message.contains("sandbox")
            || message.contains("invalid params")
            || message.contains("unknown field")
            || message.contains("unexpected field")
            || message.contains("unsupported")
    }

    private fun shouldRetryWithApprovalPolicyFallback(error: Exception): Boolean {
        val message = error.message.orEmpty().lowercase()
        return message.contains("approval")
            || message.contains("unknown variant")
            || message.contains("expected one of")
            || message.contains("onrequest")
            || message.contains("on-request")
    }

    private fun shouldRetrySkillsListWithCwdFallback(error: Exception): Boolean {
        val message = error.message.orEmpty().lowercase()
        return message.contains("invalid")
            || message.contains("unknown field")
            || message.contains("unrecognized field")
            || message.contains("missing field")
            || message.contains("expected")
            || message.contains("cwds")
    }

    private fun shouldRetryTurnStartWithoutSkillItems(error: Exception): Boolean {
        val message = error.message.orEmpty().lowercase()
        if (!message.contains("skill")) {
            return false
        }

        return message.contains("unknown")
            || message.contains("unsupported")
            || message.contains("invalid")
            || message.contains("expected")
            || message.contains("unrecognized")
            || message.contains("type")
            || message.contains("field")
    }

    private fun shouldRetryTurnStartWithoutCollaborationMode(error: Exception): Boolean {
        val message = error.message.orEmpty().lowercase()
        if (!message.contains("collaborationmode") && !message.contains("collaboration_mode")) {
            return false
        }

        return message.contains("experimentalapi")
            || message.contains("unsupported")
            || message.contains("unknown")
            || message.contains("unexpected")
            || message.contains("unrecognized")
            || message.contains("invalid")
            || message.contains("field")
            || message.contains("mode")
    }

    private fun extractItemType(item: Map<String, JsonValue>): String =
        item["type"]?.stringValue
            ?.trim()
            ?.lowercase()
            .orEmpty()

    private fun extractItemText(item: Map<String, JsonValue>): String {
        item["text"]?.stringValue?.let { if (it.isNotBlank()) return it }
        item["content"]?.stringValue?.let { if (it.isNotBlank()) return it }
        item["message"]?.stringValue?.let { if (it.isNotBlank()) return it }
        item["output"]?.stringValue?.let { if (it.isNotBlank()) return it }
        return ""
    }

    private fun extractFileChangeText(item: Map<String, JsonValue>): String {
        val path = item["path"]?.stringValue ?: item["filePath"]?.stringValue ?: ""
        val action = item["action"]?.stringValue ?: item["changeType"]?.stringValue ?: "modified"
        val additions = item["additions"]?.intValue ?: 0
        val deletions = item["deletions"]?.intValue ?: 0
        return buildString {
            append("$action: $path")
            if (additions > 0 || deletions > 0) append(" (+$additions/-$deletions)")
        }.trim()
    }

    private fun extractCommandExecutionText(item: Map<String, JsonValue>): String {
        val command = item["command"]?.stringValue
            ?: item["fullCommand"]?.stringValue
            ?: item["call"]?.stringValue
            ?: item["cmd"]?.stringValue
            ?: ""
        val output = item["output"]?.stringValue
            ?: item["outputTail"]?.stringValue
            ?: item["chunk"]?.stringValue
            ?: ""
        return buildString {
            if (command.isNotBlank()) append("$ $command")
            if (output.isNotBlank()) {
                if (isNotEmpty()) append("\n")
                append(output)
            }
        }
    }

    private fun extractCommandExecutionDetails(item: Map<String, JsonValue>): CommandExecutionDetails =
        CommandExecutionDetails(
            fullCommand = item["command"]?.stringValue
                ?: item["fullCommand"]?.stringValue
                ?: item["call"]?.stringValue
                ?: item["cmd"]?.stringValue
                ?: "",
            cwd = item["cwd"]?.stringValue,
            exitCode = item["exitCode"]?.intValue?.toInt() ?: item["exit_code"]?.intValue?.toInt(),
            durationMs = item["durationMs"]?.intValue ?: item["duration_ms"]?.intValue,
            outputTail = item["output"]?.stringValue
                ?: item["outputTail"]?.stringValue
                ?: item["chunk"]?.stringValue
                ?: ""
        )

    private fun extractIncomingDeltaText(
        payload: Map<String, JsonValue>,
        item: Map<String, JsonValue>? = null
    ): String {
        return firstStringValue(payload, "delta", "text", "summary", "part", "output", "chunk")
            ?: firstStringValue(item ?: emptyMap(), "delta", "text", "summary", "part", "output", "chunk")
            ?: ""
    }

    private fun payloadWithDelta(
        base: Map<String, JsonValue>,
        deltaText: String
    ): Map<String, JsonValue> {
        if (deltaText.isBlank()) return base
        return base.toMutableMap().apply {
            this["text"] = JsonValue.string(deltaText)
            if (this["message"] == null) {
                this["message"] = JsonValue.string(deltaText)
            }
            if (this["summary"] == null) {
                this["summary"] = JsonValue.string(deltaText)
            }
        }
    }

    private fun decodeCommandExecutionStatusText(
        payload: Map<String, JsonValue>,
        isCompleted: Boolean
    ): String {
        val command = firstStringValue(payload, "command", "fullCommand", "call", "cmd").orEmpty()
        val status = firstStringValue(payload, "status", "phase", "state")
            ?.replace('_', ' ')
            ?.trim()
            ?.replaceFirstChar { it.uppercase() }
        val output = firstStringValue(payload, "output", "outputTail", "output_tail", "delta", "text", "chunk")
            ?.trim()
            .orEmpty()

        val header = when {
            command.isNotBlank() -> "$ $command"
            !status.isNullOrBlank() -> status
            isCompleted -> "Command completed"
            else -> "Running command"
        }

        if (output.isBlank() || output == command) {
            return header
        }

        return "$header\n$output"
    }

    private fun handleThreadStatusChangedNotification(
        threadId: String,
        turnId: String?,
        payload: Map<String, JsonValue>
    ) {
        val normalizedStatus = normalizeLifecycleStatus(
            firstStringValue(
                payload["status"]?.objectValue ?: emptyMap(),
                "type",
                "statusType",
                "status_type"
            )
                ?: firstStringValue(payload, "status", "state", "phase")
                ?: firstStringValue(envelopeEventObject(payload) ?: emptyMap(), "status", "state", "phase")
        ) ?: return

        if (normalizedStatus in setOf("active", "running", "processing", "inprogress", "started", "pending")) {
            markThreadRunning(threadId)
            if (turnId != null) {
                activeTurnIdByThread[threadId] = turnId
            }
            return
        }

        val previousTurnId = activeTurnIdByThread[threadId]
        val wasRunning = isThreadRunning(threadId) || previousTurnId != null
        clearRunningState(threadId)
        activeTurnIdByThread.remove(threadId)

        when {
            normalizedStatus in setOf("completed", "complete", "done", "finished", "success", "succeeded") -> {
                _readyThreadIDs.value = _readyThreadIDs.value + threadId
                _failedThreadIDs.value = _failedThreadIDs.value - threadId
                if (wasRunning) {
                    notifyRunCompletionIfNeeded(
                        threadId = threadId,
                        turnId = turnId ?: previousTurnId,
                        result = CodexRunCompletionResult.COMPLETED
                    )
                }
            }
            normalizedStatus.contains("fail") || normalizedStatus.contains("error") -> {
                _failedThreadIDs.value = _failedThreadIDs.value + threadId
                _readyThreadIDs.value = _readyThreadIDs.value - threadId
                if (wasRunning) {
                    notifyRunCompletionIfNeeded(
                        threadId = threadId,
                        turnId = turnId ?: previousTurnId,
                        result = CodexRunCompletionResult.FAILED
                    )
                }
            }
            else -> {
                _readyThreadIDs.value = _readyThreadIDs.value - threadId
                _failedThreadIDs.value = _failedThreadIDs.value - threadId
            }
        }
    }

    private fun normalizeLifecycleStatus(rawStatus: String?): String? =
        rawStatus
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.replace("_", "")
            ?.replace("-", "")
            ?.lowercase()

    private fun legacyToolActivityLine(
        eventType: String,
        payload: Map<String, JsonValue>
    ): String? {
        return when (eventType) {
            "background_event" -> {
                firstStringValue(payload, "message", "text", "body")
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() && it.length <= 140 }
            }
            "read" -> {
                val path = firstStringValue(payload, "path", "file_path", "file")
                if (path != null) "Read $path" else "Read file"
            }
            "search" -> {
                val query = firstStringValue(payload, "query", "pattern", "regex")
                if (query != null) "Search $query" else "Search files"
            }
            "list_files" -> {
                val path = firstStringValue(payload, "path", "cwd")
                if (path != null) "List files $path" else "List files"
            }
            else -> null
        }
    }

    private fun extractContextWindowUsageSnapshot(
        payload: Map<String, JsonValue>,
        isTokenCountPayload: Boolean
    ): ContextWindowUsage? {
        val infoObject = if (isTokenCountPayload) {
            payload["info"]?.objectValue ?: payload
        } else {
            payload["usage"]?.objectValue ?: payload["info"]?.objectValue ?: payload
        }

        val preferredUsageRoot = if (isTokenCountPayload) {
            firstJsonValue(infoObject, "last_token_usage", "lastTokenUsage")?.objectValue
                ?: firstJsonValue(infoObject, "total_token_usage", "totalTokenUsage")?.objectValue
                ?: infoObject
        } else {
            payload["usage"]?.objectValue ?: infoObject
        }

        val tokenLimit = firstLongValue(
            infoObject,
            "tokenLimit",
            "token_limit",
            "model_context_window",
            "modelContextWindow",
            "context_window",
            "contextWindow"
        ) ?: return null

        val explicitTotal = firstLongValue(
            preferredUsageRoot,
            "tokensUsed",
            "tokens_used",
            "total_tokens",
            "totalTokens"
        )
        val inputTokens = firstLongValue(preferredUsageRoot, "input_tokens", "inputTokens") ?: 0L
        val outputTokens = firstLongValue(preferredUsageRoot, "output_tokens", "outputTokens") ?: 0L
        val reasoningTokens = firstLongValue(
            preferredUsageRoot,
            "reasoning_output_tokens",
            "reasoningOutputTokens"
        ) ?: 0L
        val tokensUsed = explicitTotal ?: (inputTokens + outputTokens + reasoningTokens)

        return ContextWindowUsage(
            tokensUsed = minOf(tokensUsed, tokenLimit),
            tokenLimit = tokenLimit
        )
    }

    private fun handleServerRequest(id: JsonValue, method: String, params: JsonValue?) {
        val p = params?.objectValue ?: emptyMap()

        when {
            method.contains("approval") || method.contains("confirm") -> {
                // Check auto-approve
                if (_selectedAccessMode.value == CodexAccessMode.FULL_ACCESS) {
                    messageTransport.sendResponse(id,
                        JsonValue.obj("decision" to JsonValue.string("accept")))
                    return
                }

                _pendingApproval.value = ApprovalRequest(
                    requestId = id,
                    toolName = p["tool"]?.stringValue ?: p["command"]?.stringValue ?: "Unknown",
                    description = p["description"]?.stringValue
                        ?: p["message"]?.stringValue ?: "",
                        threadId = p["threadId"]?.stringValue
                )
            }
            method == "item/tool/requestUserInput"
                || method.contains("structuredInput")
                || method.contains("structured_input")
                || method.contains("requestUserInput")
                || method.contains("request_user_input") -> {
                val threadId = firstStringValue(p, "threadId", "thread_id") ?: return
                val turnId = firstStringValue(p, "turnId", "turn_id")
                val request = historyDecoder.decodeStructuredUserInputRequest(
                    item = p,
                    fallbackRequestID = id
                ) ?: run {
                    Log.d(TAG, "Structured input request without valid questions: $method")
                    return
                }
                val messages = getThreadMessages(threadId).toMutableList()
                upsertStructuredUserInputPromptMessage(
                    threadId = threadId,
                    turnId = turnId,
                    itemId = firstStringValue(p, "itemId", "item_id") ?: "request-${request.requestIdKey}",
                    request = request,
                    messages = messages
                )
                setThreadMessages(threadId, messages)
            }
            else -> {
                Log.d(TAG, "Unhandled server request: $method")
            }
        }
    }

    // --- Sync ---

    private fun startSyncLoops() {
        syncJob?.cancel()
        syncJob = scope.launch {
            launch { threadListSyncLoop() }
            launch { activeThreadSyncLoop() }
            launch { runningBadgeWatchLoop() }
        }
    }

    private fun stopSyncLoops() {
        syncJob?.cancel()
        syncJob = null
    }

    fun refreshTrackedRunningTurnStatesNow() {
        scope.launch {
            refreshTrackedRunningTurnStates()
        }
    }

    private suspend fun refreshTrackedRunningTurnStates() {
        val candidateThreadIds = buildSet {
            addAll(_runningThreadIDs.value)
            addAll(activeTurnIdByThread.keys)
        }
        if (candidateThreadIds.isEmpty()) {
            return
        }

        candidateThreadIds.forEach { threadId ->
            try {
                applyInterruptibleTurnSnapshot(
                    threadId = threadId,
                    snapshot = readThreadTurnStateSnapshot(threadId),
                    allowBackgroundNotification = true
                )
            } catch (error: Exception) {
                Log.w(TAG, "In-flight turn refresh failed for $threadId: ${error.message}")
            }
        }
    }

    private suspend fun threadListSyncLoop() {
        while (true) {
            syncThreadList()
            val interval = if (isAppInForeground) 10_000L else 75_000L
            delay(interval)
        }
    }

    private suspend fun activeThreadSyncLoop() {
        while (true) {
            val tid = _activeThreadId.value
            if (tid != null) {
                val hasActiveOrRunningTurn = threadHasActiveOrRunningTurn(tid)
                val wantsMirroredCatchup = shouldPrioritizeMirroredRunningCatchup(tid)
                loadThreadHistoryIfNeeded(tid, forceRefresh = true)
                val interval = if (isAppInForeground) {
                    if (wantsMirroredCatchup) 1_000L       // 1s mirrored running
                    else if (hasActiveOrRunningTurn) 3_000L // 3s running
                    else 10_000L                            // 10s idle
                } else {
                    if (hasActiveOrRunningTurn) 12_000L     // 12s background running
                    else 90_000L                            // 90s background idle
                }
                delay(interval)
                continue
            }
            val interval = if (isAppInForeground) 10_000L else 90_000L
            delay(interval)
        }
    }

    private suspend fun runningBadgeWatchLoop() {
        while (true) {
            refreshInactiveRunningBadgeThreads()
            val interval = if (isAppInForeground) 2_000L else 15_000L
            delay(interval)
        }
    }

    private suspend fun refreshInactiveRunningBadgeThreads(limit: Int = 3) {
        val activeId = _activeThreadId.value
        val availableIds = _threads.value.map { it.id }.toSet()
        val candidates = _runningThreadIDs.value
            .filter { it != activeId && availableIds.contains(it) }
            .take(limit)

        for (threadId in candidates) {
            try {
                applyInterruptibleTurnSnapshot(
                    threadId = threadId,
                    snapshot = readThreadTurnStateSnapshot(threadId),
                    allowBackgroundNotification = true
                )
            } catch (e: Exception) {
                Log.w(TAG, "Running badge refresh failed for $threadId: ${e.message}")
            }
        }
    }

    suspend fun syncThreadList() {
        connectionPresentationCoordinator.setLoadingThreads(true)
        try {
            val activeThreads = fetchServerThreads(limit = THREAD_LIST_LIMIT)

            val archivedThreads = try {
                fetchServerThreads(limit = THREAD_LIST_LIMIT, archived = true)
            } catch (e: Exception) {
                Log.w(TAG, "thread/list archived fetch failed (non-fatal): ${e.message}")
                emptyList()
            }

            reconcileLocalThreadsWithServer(activeThreads, archivedThreads)
        } catch (e: Exception) {
            Log.e(TAG, "Sync thread list failed: ${e.message}")
        } finally {
            connectionPresentationCoordinator.setLoadingThreads(false)
        }
    }

    private suspend fun fetchServerThreads(limit: Int? = null, archived: Boolean = false): List<CodexThread> {
        val attempts = listOf(true, false)
        var lastError: Exception? = null

        for (includeSourceKinds in attempts) {
            try {
                return fetchServerThreadsInternal(
                    limit = limit,
                    archived = archived,
                    includeSourceKinds = includeSourceKinds
                )
            } catch (e: Exception) {
                lastError = e
            }
        }

        throw lastError ?: IllegalStateException("thread/list failed")
    }

    private suspend fun fetchServerThreadsInternal(
        limit: Int?,
        archived: Boolean,
        includeSourceKinds: Boolean
    ): List<CodexThread> {
        val allThreads = mutableListOf<CodexThread>()
        var nextCursor: JsonValue = JsonValue.nullValue
        var hasRequestedFirstPage = false

        do {
            val params = mutableMapOf<String, JsonValue>(
                "cursor" to nextCursor
            )
            if (includeSourceKinds) {
                params["sourceKinds"] = JsonValue.array(
                    JsonValue.string("cli"),
                    JsonValue.string("vscode"),
                    JsonValue.string("appServer"),
                    JsonValue.string("exec"),
                    JsonValue.string("subagent"),
                    JsonValue.string("unknown")
                )
            }
            if (limit != null) {
                params["limit"] = JsonValue.int(limit.toLong())
            }
            if (archived) {
                params["archived"] = JsonValue.bool(true)
            }

            val response = requireSuccessfulResponse(
                messageTransport.sendRequest("thread/list", JsonValue.ObjectValue(params)),
                "thread/list"
            )
            val resultObject = response.result?.objectValue
                ?: throw IllegalStateException("thread/list response missing payload")
            val page = resultObject["data"]?.arrayValue
                ?: resultObject["items"]?.arrayValue
                ?: resultObject["threads"]?.arrayValue
                ?: throw IllegalStateException("thread/list response missing data array")

            allThreads.addAll(page.mapNotNull { t ->
                val elem = json.parseToJsonElement(json.encodeToString(JsonValue.serializer(), t))
                CodexThread.fromJson(json, elem)
            })
            nextCursor = resultObject["nextCursor"] ?: resultObject["next_cursor"] ?: JsonValue.nullValue
            hasRequestedFirstPage = true
        } while (shouldContinueThreadListPagination(nextCursor, limit, hasRequestedFirstPage))

        return allThreads
    }

    private fun shouldContinueThreadListPagination(
        nextCursor: JsonValue,
        limit: Int?,
        hasRequestedFirstPage: Boolean
    ): Boolean {
        if (!hasRequestedFirstPage || limit != null) {
            return false
        }

        return when (nextCursor) {
            JsonValue.nullValue -> false
            is JsonValue.StringValue -> nextCursor.value.trim().isNotEmpty()
            else -> true
        }
    }

    private fun reconcileLocalThreadsWithServer(
        serverThreads: List<CodexThread>,
        serverArchivedThreads: List<CodexThread> = emptyList()
    ) {
        val previousActiveThreadId = _activeThreadId.value
        val localById = _threads.value.associateBy { it.id }
        val persistedArchivedIDs = locallyArchivedThreadIDs()
        val persistedDeletedIDs = locallyDeletedThreadIDs()
        val merged = linkedMapOf<String, CodexThread>()

        for (serverThread in serverThreads) {
            if (persistedDeletedIDs.contains(serverThread.id)) {
                continue
            }

            val localThread = localById[serverThread.id]
            val mergedThread = applyPersistedThreadRename(
                applyPersistedThreadProjectPath(
                    mergeThread(serverThread, localThread)
                        .copy(
                            syncState = when {
                                localThread?.syncState == CodexThreadSyncState.ARCHIVED_LOCAL -> CodexThreadSyncState.ARCHIVED_LOCAL
                                persistedArchivedIDs.contains(serverThread.id) -> CodexThreadSyncState.ARCHIVED_LOCAL
                                else -> CodexThreadSyncState.LIVE
                            }
                        )
                )
            )
            merged[mergedThread.id] = mergedThread
        }

        for (serverThread in serverArchivedThreads) {
            if (persistedDeletedIDs.contains(serverThread.id) || merged.containsKey(serverThread.id)) {
                continue
            }

            val localThread = localById[serverThread.id]
            val archivedThread = applyPersistedThreadRename(
                applyPersistedThreadProjectPath(
                    mergeThread(serverThread, localThread)
                        .copy(syncState = CodexThreadSyncState.ARCHIVED_LOCAL)
                )
            )
            addLocallyArchivedThreadID(archivedThread.id)
            merged[archivedThread.id] = archivedThread
        }

        for (localThread in _threads.value) {
            if (merged.containsKey(localThread.id) || persistedDeletedIDs.contains(localThread.id)) {
                continue
            }
            merged[localThread.id] = applyPersistedThreadRename(
                applyPersistedThreadProjectPath(localThread)
            )
        }

        _threads.value = sortThreads(merged.values.toList())

        val resolvedActiveThreadId = _activeThreadId.value
            ?.takeIf { activeId ->
                _threads.value.any { thread ->
                    thread.id == activeId && thread.syncState == CodexThreadSyncState.LIVE
                }
            }
            ?: persistedActiveThreadId()
                ?.takeIf { persistedId ->
                    _threads.value.any { thread ->
                        thread.id == persistedId && thread.syncState == CodexThreadSyncState.LIVE
                    }
                }
            ?: _threads.value.firstOrNull {
                it.syncState == CodexThreadSyncState.LIVE
            }?.id

        if (_activeThreadId.value != resolvedActiveThreadId) {
            _activeThreadId.value = resolvedActiveThreadId
        }
        persistActiveThreadId(_activeThreadId.value)

        if (resolvedActiveThreadId != null && resolvedActiveThreadId != previousActiveThreadId) {
            requestThreadHistoryLoad(resolvedActiveThreadId)
        }

        if (pendingNotificationOpenThreadID != null) {
            scope.launch {
                routePendingNotificationOpenIfPossible(refreshIfNeeded = false)
            }
        }
    }

    private fun mergeThread(incoming: CodexThread, existing: CodexThread?): CodexThread {
        val persistedForkOrigin = persistedForkOrigin(incoming.id)
        if (existing == null) {
            return incoming.copy(
                forkedFromThreadId = incoming.forkedFromThreadId ?: persistedForkOrigin
            )
        }

        return incoming.copy(
            title = incoming.title ?: existing.title,
            name = incoming.name ?: existing.name,
            preview = incoming.preview ?: existing.preview,
            createdAtRaw = incoming.createdAtRaw ?: existing.createdAtRaw,
            updatedAtRaw = incoming.updatedAtRaw ?: existing.updatedAtRaw,
            cwd = incoming.cwd ?: existing.cwd,
            metadata = incoming.metadata ?: existing.metadata,
            forkedFromThreadId = incoming.forkedFromThreadId ?: existing.forkedFromThreadId ?: persistedForkOrigin,
            parentThreadId = incoming.parentThreadId ?: existing.parentThreadId,
            agentId = incoming.agentId ?: existing.agentId,
            agentNickname = incoming.agentNickname ?: existing.agentNickname,
            agentRole = incoming.agentRole ?: existing.agentRole,
            model = incoming.model ?: existing.model,
            modelProvider = incoming.modelProvider ?: existing.modelProvider,
            syncState = if (incoming.syncState == CodexThreadSyncState.ARCHIVED_LOCAL || existing.syncState == CodexThreadSyncState.ARCHIVED_LOCAL) {
                CodexThreadSyncState.ARCHIVED_LOCAL
            } else {
                CodexThreadSyncState.LIVE
            }
        )
    }

    private fun sortThreads(value: List<CodexThread>): List<CodexThread> =
        value.sortedByDescending { thread ->
            thread.updatedAt.takeIf { it > 0L } ?: thread.createdAt.takeIf { it > 0L } ?: 0L
        }

    private fun requestImmediateActiveThreadSync(threadId: String? = null) {
        val resolvedThreadId = threadId ?: _activeThreadId.value ?: return
        if (!isConnected.value || !isRuntimeReady()) {
            return
        }

        scope.launch {
            loadThreadHistoryIfNeeded(resolvedThreadId, forceRefresh = true)
        }
    }

    fun refreshActiveThreadNow(threadId: String? = null) {
        requestImmediateActiveThreadSync(threadId)
    }

    private suspend fun syncThreadHistory(threadId: String) {
        val result = messageTransport.sendRequest(
            "thread/read",
            JsonValue.obj(
                "threadId" to JsonValue.string(threadId),
                "includeTurns" to JsonValue.bool(true)
            )
        )
        applyInterruptibleTurnSnapshot(
            threadId = threadId,
            snapshot = decodeInterruptibleTurnSnapshot(result.result),
            allowBackgroundNotification = true
        )
        result.result
            ?.objectValue
            ?.get("thread")
            ?.let { CodexThread.fromJson(json, it.toJsonElement()) }
            ?.let(::upsertThread)

        val messages = historyDecoder.decodeMessagesFromThreadRead(threadId, result.result)
        if (messages.isNotEmpty()) {
            setThreadMessages(
                threadId,
                mergeHistoryMessages(threadId, messages)
            )
        }
        recoverAIChangeSetsFromThreadRead(threadId, result.result, messages)
    }

    private fun requestThreadHistoryLoad(
        threadId: String,
        forceRefresh: Boolean = false,
        markHydratedWhenNotMaterialized: Boolean = true
    ) {
        threadHistoryHydrator.launchLoad(
            threadId = threadId,
            forceRefresh = forceRefresh,
            markHydratedWhenNotMaterialized = markHydratedWhenNotMaterialized
        )
    }

    private suspend fun loadThreadHistoryIfNeeded(
        threadId: String,
        forceRefresh: Boolean = false,
        markHydratedWhenNotMaterialized: Boolean = true
    ) {
        threadHistoryHydrator.loadIfNeeded(
            threadId = threadId,
            forceRefresh = forceRefresh,
            markHydratedWhenNotMaterialized = markHydratedWhenNotMaterialized
        )
    }

    private fun shouldMarkThreadHistoryHydratedAfterError(error: Exception): Boolean {
        return error is RpcRequestException
            && error.method == "thread/read"
            && error.code == -32600
    }

    fun assistantRevertPresentation(
        message: CodexMessage,
        workingDirectory: String?
    ): AssistantRevertPresentation? {
        if (!message.isAssistant) {
            return null
        }

        val changeSet = aiChangeSetForAssistantMessage(message) ?: return null
        return when (changeSet.status) {
            AIChangeSetStatus.READY -> {
                val normalizedWorkingDirectory = normalizeWorkingDirectory(workingDirectory)
                if (normalizedWorkingDirectory == null) {
                    AssistantRevertPresentation(
                        title = "Cannot undo",
                        isEnabled = false,
                        helperText = "The selected local folder is not available on this Mac.",
                        riskLevel = AssistantRevertRiskLevel.BLOCKED
                    )
                } else if (hasActiveRunInRepo(changeSet.repoRoot ?: normalizedWorkingDirectory)) {
                    AssistantRevertPresentation(
                        title = "Cannot undo",
                        isEnabled = false,
                        helperText = "Finish the active run in this repo before undoing this response.",
                        riskLevel = AssistantRevertRiskLevel.BLOCKED
                    )
                } else {
                    val overlappingFiles = overlappingFilesForChangeSet(changeSet, normalizedWorkingDirectory)
                    if (overlappingFiles.isNotEmpty()) {
                        AssistantRevertPresentation(
                            title = "Undo changes",
                            isEnabled = true,
                            helperText = "Review overlapping files before undoing this response.",
                            riskLevel = AssistantRevertRiskLevel.WARNING,
                            warningText = "Other chats also changed ${overlappingFiles.size} of these file${if (overlappingFiles.size == 1) "" else "s"}.",
                            overlappingFiles = overlappingFiles
                        )
                    } else {
                        AssistantRevertPresentation(
                            title = "Undo changes",
                            isEnabled = true,
                            helperText = "Only changes from this response will be reverted unless later edits overlap.",
                            riskLevel = AssistantRevertRiskLevel.SAFE
                        )
                    }
                }
            }
            AIChangeSetStatus.COLLECTING -> AssistantRevertPresentation(
                title = "Undo changes",
                isEnabled = false,
                helperText = "This response is still collecting its final patch.",
                riskLevel = AssistantRevertRiskLevel.BLOCKED
            )
            AIChangeSetStatus.REVERTED -> AssistantRevertPresentation(
                title = "Already undone",
                isEnabled = false,
                riskLevel = AssistantRevertRiskLevel.BLOCKED
            )
            AIChangeSetStatus.FAILED, AIChangeSetStatus.NOT_REVERTABLE -> AssistantRevertPresentation(
                title = "Cannot undo",
                isEnabled = false,
                helperText = firstNonEmptyString(
                    changeSet.revertMetadata.lastRevertError,
                    changeSet.unsupportedReasons.firstOrNull()
                ),
                riskLevel = AssistantRevertRiskLevel.BLOCKED
            )
        }
    }

    fun readyAIChangeSetForMessage(message: CodexMessage): AIChangeSet? =
        aiChangeSetForAssistantMessage(message)
            ?.takeIf { it.status == AIChangeSetStatus.READY }

    fun diffableAIChangeSetForMessage(message: CodexMessage): AIChangeSet? =
        aiChangeSetForAssistantMessage(message)
            ?.takeIf { !it.forwardUnifiedPatch.isNullOrBlank() }

    suspend fun previewRevert(changeSetId: String, workingDirectory: String): RevertPreviewResult {
        val changeSet = aiChangeSetsById[changeSetId]
            ?: throw IllegalArgumentException("This response does not have a tracked patch yet.")
        val normalizedWorkingDirectory = normalizeWorkingDirectory(workingDirectory)
            ?: throw IllegalStateException("The selected local folder is not available on this Mac.")
        val patch = changeSet.forwardUnifiedPatch?.trim().orEmpty()
        if (patch.isEmpty()) {
            throw IllegalStateException("This response cannot be auto-reverted because no exact patch was captured.")
        }

        return try {
            val response = requireSuccessfulResponse(
                messageTransport.sendRequest(
                    "workspace/revertPatchPreview",
                    JsonValue.obj(
                        "cwd" to JsonValue.string(normalizedWorkingDirectory),
                        "forwardPatch" to JsonValue.string(patch)
                    )
                ),
                "workspace/revertPatchPreview"
            )
            json.decodeFromString(
                RevertPreviewResult.serializer(),
                json.encodeToString(JsonValue.serializer(), response.result ?: JsonValue.obj())
            )
        } catch (error: RpcRequestException) {
            throw IllegalStateException(
                workspaceUserMessage(error.errorCode, error.rpcMessage)
            )
        }
    }

    suspend fun applyRevert(changeSetId: String, workingDirectory: String): RevertApplyResult {
        val changeSet = aiChangeSetsById[changeSetId]
            ?: throw IllegalArgumentException("This response does not have a tracked patch yet.")
        val normalizedWorkingDirectory = normalizeWorkingDirectory(workingDirectory)
            ?: throw IllegalStateException("The selected local folder is not available on this Mac.")
        val patch = changeSet.forwardUnifiedPatch?.trim().orEmpty()
        if (patch.isEmpty()) {
            throw IllegalStateException("This response cannot be auto-reverted because no exact patch was captured.")
        }

        markChangeSetRevertAttempt(changeSetId)

        val applyResult = try {
            val response = requireSuccessfulResponse(
                messageTransport.sendRequest(
                    "workspace/revertPatchApply",
                    JsonValue.obj(
                        "cwd" to JsonValue.string(normalizedWorkingDirectory),
                        "forwardPatch" to JsonValue.string(patch)
                    )
                ),
                "workspace/revertPatchApply"
            )
            json.decodeFromString(
                RevertApplyResult.serializer(),
                json.encodeToString(JsonValue.serializer(), response.result ?: JsonValue.obj())
            )
        } catch (error: RpcRequestException) {
            val message = workspaceUserMessage(error.errorCode, error.rpcMessage)
            recordChangeSetError(changeSetId, message)
            throw IllegalStateException(message)
        }

        rememberRepoRoot(applyResult.status?.repoRoot, normalizedWorkingDirectory)
        if (applyResult.success) {
            markChangeSetReverted(changeSetId)
            changeSet.threadId?.let { threadId ->
                appendMessage(
                    threadId,
                    CodexMessage(
                        threadId = threadId,
                        role = CodexMessageRole.SYSTEM,
                        kind = CodexMessageKind.CHAT,
                        text = "Reverted changes from this response.",
                        turnId = changeSet.turnId,
                        orderIndex = CodexMessageOrderCounter.next(),
                        deliveryState = CodexMessageDeliveryState.CONFIRMED
                    )
                )
            }
        } else {
            recordChangeSetError(
                changeSetId,
                firstNonEmptyString(
                    applyResult.unsupportedReasons.firstOrNull(),
                    applyResult.conflicts.firstOrNull()?.message,
                    applyResult.stagedFiles.firstOrNull()?.let {
                        "Some targeted files have staged changes. Unstage them first to keep revert predictable."
                    }
                ) ?: "Patch revert failed."
            )
        }

        return applyResult
    }

    // --- Context Window & Rate Limits ---

    fun refreshContextWindowUsage(threadId: String) {
        scope.launch {
            try {
                val result = messageTransport.sendRequest("thread/contextWindow/read", JsonValue.obj(
                    "threadId" to JsonValue.string(threadId)
                ))
                val usage = result.result?.objectValue?.get("usage")?.objectValue
                if (usage != null) {
                    _contextWindowUsage.value = ContextWindowUsage(
                        tokensUsed = usage["tokensUsed"]?.intValue ?: 0,
                        tokenLimit = usage["tokenLimit"]?.intValue ?: 0
                    )
                }
            } catch (_: Exception) {}
        }
    }

    fun refreshRateLimits() {
        scope.launch {
            try {
                val result = messageTransport.sendRequest("account/rateLimits/read")
                val buckets = result.result?.objectValue?.get("buckets")?.arrayValue
                if (buckets != null) {
                    _rateLimitBuckets.value = buckets.mapNotNull { b ->
                        try { json.decodeFromString(CodexRateLimitBucket.serializer(),
                            json.encodeToString(JsonValue.serializer(), b)) }
                        catch (_: Exception) { null }
                    }
                }
            } catch (_: Exception) {}
        }
    }

    suspend fun startOrResumeGPTLoginOnPhone(): String? {
        if (!isConnected.value) {
            _gptAccountErrorMessage.value = "Connect to your bridge before opening ChatGPT sign-in."
            return null
        }

        _gptAccountErrorMessage.value = null

        return try {
            val response = requireSuccessfulResponse(
                messageTransport.sendRequest(
                    "account/login/start",
                    JsonValue.obj("type" to JsonValue.string("chatgpt"))
                ),
                "account/login/start"
            )
            val payload = response.result?.objectValue
                ?: throw IllegalStateException("account/login/start response missing payload")
            val loginId = firstStringValue(payload, "loginId", "login_id")
                ?: throw IllegalStateException("account/login/start response missing login id")
            val authUrl = firstStringValue(payload, "authUrl", "auth_url")
                ?: throw IllegalStateException("account/login/start response missing auth URL")

            pendingGPTLoginId = loginId
            _gptAccountSnapshot.value = _gptAccountSnapshot.value.copy(
                status = CodexGPTAccountStatus.LOGIN_PENDING,
                loginInFlight = true,
                tokenReady = false,
                expiresAt = firstStringValue(payload, "expiresAt", "expires_at"),
                updatedAtEpochMs = System.currentTimeMillis()
            )
            authUrl
        } catch (e: Exception) {
            _gptAccountErrorMessage.value = e.message ?: "Unable to start ChatGPT login."
            null
        }
    }

    suspend fun cancelGPTLogin() {
        try {
            if (isConnected.value) {
                pendingGPTLoginId?.let { loginId ->
                    messageTransport.sendRequest(
                        "account/login/cancel",
                        JsonValue.obj("loginId" to JsonValue.string(loginId))
                    )
                }
            }
            pendingGPTLoginId = null
            if (!_gptAccountSnapshot.value.isAuthenticated) {
                _gptAccountSnapshot.value = _gptAccountSnapshot.value.copy(
                    status = CodexGPTAccountStatus.NOT_LOGGED_IN,
                    loginInFlight = false,
                    needsReauth = false,
                    tokenReady = false,
                    expiresAt = null,
                    updatedAtEpochMs = System.currentTimeMillis()
                )
            }
            _gptAccountErrorMessage.value = null
        } catch (e: Exception) {
            _gptAccountErrorMessage.value = e.message ?: "Unable to cancel ChatGPT login."
        }
    }

    suspend fun logoutGPTAccount() {
        try {
            if (isConnected.value) {
                messageTransport.sendRequest("account/logout")
            }
            pendingGPTLoginId = null
            _gptAccountSnapshot.value = CodexGPTAccountSnapshot(
                status = CodexGPTAccountStatus.NOT_LOGGED_IN,
                updatedAtEpochMs = System.currentTimeMillis()
            )
            _gptAccountErrorMessage.value = null
        } catch (e: Exception) {
            _gptAccountErrorMessage.value = e.message ?: "Unable to log out."
        }
    }

    // --- Git Actions ---

    suspend fun gitStatus(threadId: String, cwd: String? = null): GitRepoSyncResult? {
        return try {
            val params = gitRequestParams(threadId = threadId, cwd = cwd) ?: return null
            val result = messageTransport.sendRequest("git/status", params)
            json.decodeFromString(GitRepoSyncResult.serializer(),
                json.encodeToString(JsonValue.serializer(), result.result ?: return null))
                .also { rememberRepoRoot(it.repoRoot, cwd ?: thread(threadId)?.gitWorkingDirectory) }
        } catch (_: Exception) { null }
    }

    suspend fun gitDiff(threadId: String, cwd: String? = null): GitDiffResult {
        val result = sendGitRequestResult(
            method = "git/diff",
            threadId = threadId,
            cwd = cwd
        )
        return json.decodeFromString(
            GitDiffResult.serializer(),
            json.encodeToString(JsonValue.serializer(), result)
        )
    }

    suspend fun gitCommit(threadId: String, message: String): GitCommitResult? {
        return try {
            val params = gitRequestParams(
                threadId = threadId,
                extra = listOf("message" to JsonValue.string(message))
            ) ?: return null
            val result = messageTransport.sendRequest("git/commit", params)
            json.decodeFromString(GitCommitResult.serializer(),
                json.encodeToString(JsonValue.serializer(), result.result ?: return null))
        } catch (_: Exception) { null }
    }

    suspend fun gitPush(threadId: String): GitPushResult? {
        return try {
            val params = gitRequestParams(threadId = threadId) ?: return null
            val result = messageTransport.sendRequest("git/push", params)
            json.decodeFromString(GitPushResult.serializer(),
                json.encodeToString(JsonValue.serializer(), result.result ?: return null))
        } catch (_: Exception) { null }
    }

    suspend fun gitPull(threadId: String): GitPullResult? {
        return try {
            val params = gitRequestParams(threadId = threadId) ?: return null
            val result = messageTransport.sendRequest("git/pull", params)
            json.decodeFromString(GitPullResult.serializer(),
                json.encodeToString(JsonValue.serializer(), result.result ?: return null))
        } catch (_: Exception) { null }
    }

    suspend fun gitBranchesWithStatus(
        threadId: String,
        cwd: String? = null
    ): GitBranchesWithStatusResult {
        val result = sendGitRequestResult(
            method = "git/branchesWithStatus",
            threadId = threadId,
            cwd = cwd
        )
        return json.decodeFromString(
            GitBranchesWithStatusResult.serializer(),
            json.encodeToString(JsonValue.serializer(), result)
        ).also { rememberRepoRoot(it.status?.repoRoot, cwd ?: thread(threadId)?.gitWorkingDirectory) }
    }

    suspend fun gitCreateWorktree(
        threadId: String,
        name: String,
        baseBranch: String,
        changeTransfer: GitWorktreeChangeTransferMode = GitWorktreeChangeTransferMode.COPY,
        cwd: String? = null
    ): GitCreateWorktreeResult {
        val result = sendGitRequestResult(
            method = "git/createWorktree",
            threadId = threadId,
            cwd = cwd,
            extra = listOf(
                "name" to JsonValue.string(name.trim()),
                "baseBranch" to JsonValue.string(baseBranch.trim()),
                "changeTransfer" to JsonValue.string(changeTransfer.name.lowercase())
            )
        )
        return json.decodeFromString(
            GitCreateWorktreeResult.serializer(),
            json.encodeToString(JsonValue.serializer(), result)
        )
    }

    suspend fun gitCheckout(
        threadId: String,
        branch: String,
        cwd: String? = null
    ): GitCheckoutResult {
        val result = sendGitRequestResult(
            method = "git/checkout",
            threadId = threadId,
            cwd = cwd,
            extra = listOf("branch" to JsonValue.string(branch.trim()))
        )
        return json.decodeFromString(
            GitCheckoutResult.serializer(),
            json.encodeToString(JsonValue.serializer(), result)
        ).also { rememberRepoRoot(it.status?.repoRoot, cwd ?: thread(threadId)?.gitWorkingDirectory) }
    }

    suspend fun gitRemoveManagedWorktree(
        cwd: String,
        branch: String? = null
    ) {
        val extraParams = branch
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { listOf("branch" to JsonValue.string(it)) }
            ?: emptyList()
        sendGitRequestResult(
            method = "git/removeWorktree",
            cwd = cwd,
            extra = extraParams
        )
    }

    suspend fun createPullRequestUrl(cwd: String, currentBranchHint: String? = null): String? {
        return try {
            val remoteResult = requireSuccessfulResponse(
                messageTransport.sendRequest(
                    "git/remoteUrl",
                    JsonValue.obj("cwd" to JsonValue.string(cwd))
                ),
                "git/remoteUrl"
            )
            val remote = json.decodeFromString(
                GitRemoteUrlResult.serializer(),
                json.encodeToString(JsonValue.serializer(), remoteResult.result ?: return null)
            )
            val ownerRepo = remote.ownerRepo?.trim().takeUnless { it.isNullOrEmpty() } ?: return null

            val branchesResult = requireSuccessfulResponse(
                messageTransport.sendRequest(
                    "git/branches",
                    JsonValue.obj("cwd" to JsonValue.string(cwd))
                ),
                "git/branches"
            )
            val branches = json.decodeFromString(
                GitBranchesResult.serializer(),
                json.encodeToString(JsonValue.serializer(), branchesResult.result ?: return null)
            )
            val branch = currentBranchHint?.trim().takeUnless { it.isNullOrEmpty() }
                ?: branches.current?.trim().takeUnless { it.isNullOrEmpty() }
                ?: return null
            val base = branches.default?.trim().takeUnless { it.isNullOrEmpty() } ?: return null

            if (branch == base) {
                return null
            }

            "https://github.com/$ownerRepo/compare/${encodePathSegment(base)}...${encodePathSegment(branch)}?expand=1"
        } catch (e: Exception) {
            Log.w(TAG, "Create PR URL failed: ${e.message}")
            null
        }
    }

    // --- Helpers ---

    private fun decodeBridgeGPTAccountSnapshot(payload: Map<String, JsonValue>): CodexGPTAccountSnapshot {
        val parsedStatus = decodeGPTAccountStatus(firstStringValue(payload, "status", "state"))
        val bridgeReportedPendingLogin = firstBoolValue(payload, "loginInFlight", "login_in_flight") ?: false
        val needsReauth = firstBoolValue(payload, "needsReauth", "needs_reauth") ?: false

        val resolvedStatus = when {
            parsedStatus == CodexGPTAccountStatus.AUTHENTICATED || parsedStatus == CodexGPTAccountStatus.EXPIRED ->
                parsedStatus
            parsedStatus == CodexGPTAccountStatus.UNKNOWN
                && firstStringValue(payload, "authToken", "auth_token") != null
                && !needsReauth ->
                CodexGPTAccountStatus.AUTHENTICATED
            parsedStatus == CodexGPTAccountStatus.NOT_LOGGED_IN && bridgeReportedPendingLogin && !needsReauth ->
                CodexGPTAccountStatus.LOGIN_PENDING
            parsedStatus == CodexGPTAccountStatus.UNKNOWN && bridgeReportedPendingLogin ->
                CodexGPTAccountStatus.LOGIN_PENDING
            parsedStatus == CodexGPTAccountStatus.UNKNOWN ->
                CodexGPTAccountStatus.NOT_LOGGED_IN
            else -> parsedStatus
        }

        val tokenReady = firstBoolValue(payload, "tokenReady", "token_ready")
            ?: (resolvedStatus == CodexGPTAccountStatus.AUTHENTICATED && !needsReauth)

        return CodexGPTAccountSnapshot(
            status = resolvedStatus,
            email = firstStringValue(payload, "email"),
            planType = firstStringValue(payload, "planType", "plan_type")
                ?.replaceFirstChar { it.uppercase() },
            loginInFlight = bridgeReportedPendingLogin || resolvedStatus == CodexGPTAccountStatus.LOGIN_PENDING,
            needsReauth = needsReauth || resolvedStatus == CodexGPTAccountStatus.EXPIRED,
            tokenReady = tokenReady,
            expiresAt = firstStringValue(payload, "expiresAt", "expires_at"),
            updatedAtEpochMs = System.currentTimeMillis()
        )
    }

    private fun decodeGPTAccountStatus(value: String?): CodexGPTAccountStatus {
        return when (value?.trim()?.lowercase()) {
            "authenticated", "logged_in", "loggedin", "connected" -> CodexGPTAccountStatus.AUTHENTICATED
            "loginpending", "login_pending", "pending", "pending_login" -> CodexGPTAccountStatus.LOGIN_PENDING
            "expired", "needs_reauth", "needsreauth", "reauth_required" -> CodexGPTAccountStatus.EXPIRED
            "not_logged_in", "notloggedin", "signed_out", "logged_out", "unauthenticated" ->
                CodexGPTAccountStatus.NOT_LOGGED_IN
            "unavailable", "offline" -> CodexGPTAccountStatus.UNAVAILABLE
            null -> CodexGPTAccountStatus.UNKNOWN
            else -> CodexGPTAccountStatus.UNKNOWN
        }
    }

    private fun gitRequestParams(
        threadId: String? = null,
        cwd: String? = null,
        extra: List<Pair<String, JsonValue>> = emptyList()
    ): JsonValue.ObjectValue? {
        val resolvedCwd = resolveGitWorkingDirectory(threadId = threadId, cwd = cwd) ?: return null
        val params = mutableListOf<Pair<String, JsonValue>>(
            "cwd" to JsonValue.string(resolvedCwd)
        )
        threadId
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { params += "threadId" to JsonValue.string(it) }
        params += extra
        return JsonValue.obj(*params.toTypedArray())
    }

    private fun resolveGitWorkingDirectory(threadId: String? = null, cwd: String? = null): String? {
        cwd?.trim()?.takeIf { it.isNotEmpty() }?.let { return it }
        val normalizedThreadId = threadId?.trim()?.takeIf { it.isNotEmpty() } ?: return null
        return _threads.value
            .firstOrNull { it.id == normalizedThreadId }
            ?.gitWorkingDirectory
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
    }

    private fun firstStringValue(payload: Map<String, JsonValue>, vararg keys: String): String? {
        keys.forEach { key ->
            payload[key]?.stringValue?.trim()?.takeIf { it.isNotEmpty() }?.let { return it }
        }
        return null
    }

    private fun firstBoolValue(payload: Map<String, JsonValue>, vararg keys: String): Boolean? {
        keys.forEach { key ->
            payload[key]?.boolValue?.let { return it }
        }
        return null
    }

    private fun firstLongValue(payload: Map<String, JsonValue>, vararg keys: String): Long? {
        keys.forEach { key ->
            payload[key]?.intValue?.let { return it }
            payload[key]?.stringValue
                ?.trim()
                ?.takeIf { it.isNotEmpty() }
                ?.toLongOrNull()
                ?.let { return it }
        }
        return null
    }

    private fun firstJsonValue(payload: Map<String, JsonValue>, vararg keys: String): JsonValue? {
        keys.forEach { key ->
            payload[key]?.let { return it }
        }
        return null
    }

    private fun encodePathSegment(value: String): String =
        URLEncoder.encode(value, Charsets.UTF_8.name()).replace("+", "%20")

    private fun thread(threadId: String): CodexThread? =
        _threads.value.firstOrNull { it.id == threadId }

    private fun upsertThread(incomingThread: CodexThread) {
        val resolvedIncoming = incomingThread.copy(
            forkedFromThreadId = incomingThread.forkedFromThreadId ?: persistedForkOrigin(incomingThread.id)
        )
        val resolvedThread = applyPersistedThreadRename(
            applyPersistedThreadProjectPath(
                mergeThread(resolvedIncoming, thread(resolvedIncoming.id))
            )
        )
        rememberForkOriginIfNeeded(
            sourceThreadId = resolvedThread.forkedFromThreadId,
            forkedThreadId = resolvedThread.id
        )
        val current = _threads.value.toMutableList()
        val idx = current.indexOfFirst { it.id == resolvedThread.id }
        if (idx >= 0) {
            current[idx] = resolvedThread
        } else {
            current.add(resolvedThread)
        }
        _threads.value = sortThreads(current)

        if (pendingNotificationOpenThreadID != null) {
            scope.launch {
                routePendingNotificationOpenIfPossible(refreshIfNeeded = false)
            }
        }
    }

    fun handleNotificationOpen(threadId: String, turnId: String?) {
        val normalizedThreadId = threadId.trim().takeIf { it.isNotEmpty() } ?: return
        pendingNotificationOpenThreadID = normalizedThreadId
        pendingNotificationOpenTurnID = turnId?.trim()?.takeIf { it.isNotEmpty() }
        scope.launch {
            routePendingNotificationOpenIfPossible(refreshIfNeeded = true)
        }
    }

    fun dismissMissingNotificationThreadPrompt() {
        _missingNotificationThreadPrompt.value = null
    }

    suspend fun routePendingNotificationOpenIfPossible(refreshIfNeeded: Boolean = true): Boolean {
        val pendingThreadId = pendingNotificationOpenThreadID
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?: return false

        if (hasNotificationRoutingCandidate(pendingThreadId)) {
            _missingNotificationThreadPrompt.value = null
            pendingNotificationOpenThreadID = null
            pendingNotificationOpenTurnID = null
            selectThread(pendingThreadId)
            return true
        }

        if (!isConnected.value) {
            return false
        }

        if (refreshIfNeeded) {
            syncThreadList()
        }

        if (hasNotificationRoutingCandidate(pendingThreadId)) {
            _missingNotificationThreadPrompt.value = null
            pendingNotificationOpenThreadID = null
            pendingNotificationOpenTurnID = null
            selectThread(pendingThreadId)
            return true
        }

        if (isNotificationRouteKnownMissing(pendingThreadId)) {
            if (_activeThreadId.value == null || _activeThreadId.value == pendingThreadId) {
                _activeThreadId.value = firstLiveThreadID()
                persistActiveThreadId(_activeThreadId.value)
            }
            _missingNotificationThreadPrompt.value = CodexMissingNotificationThreadPrompt(pendingThreadId)
        }

        return false
    }

    private fun notifyRunCompletionIfNeeded(
        threadId: String,
        turnId: String?,
        result: CodexRunCompletionResult
    ) {
        if (isAppInForeground) {
            return
        }

        val nowMillis = System.currentTimeMillis()
        pruneRunCompletionNotificationDedupe(nowMillis)
        val dedupeKey = runCompletionNotificationDedupeKey(threadId, turnId, result, nowMillis)
        if (runCompletionNotificationDedupedAt.containsKey(dedupeKey)) {
            return
        }

        runCompletionNotificationDedupedAt[dedupeKey] = nowMillis
        runCompletionNotifier.postRunCompletionNotification(
            threadId = threadId,
            threadTitle = thread(threadId)?.displayTitle ?: "Conversation",
            turnId = turnId,
            result = result
        )
    }

    private fun hasNotificationRoutingCandidate(threadId: String): Boolean {
        val thread = thread(threadId) ?: return false
        return thread.syncState != CodexThreadSyncState.ARCHIVED_LOCAL
    }

    private fun isNotificationRouteKnownMissing(threadId: String): Boolean =
        thread(threadId)?.syncState == CodexThreadSyncState.ARCHIVED_LOCAL

    private fun firstLiveThreadID(): String? =
        _threads.value.firstOrNull { it.syncState == CodexThreadSyncState.LIVE }?.id

    private fun runCompletionNotificationDedupeKey(
        threadId: String,
        turnId: String?,
        result: CodexRunCompletionResult,
        nowMillis: Long
    ): String {
        return if (!turnId.isNullOrBlank()) {
            "$threadId|$turnId|${result.name}"
        } else {
            "$threadId|${result.name}|${nowMillis / 30_000L}"
        }
    }

    private fun pruneRunCompletionNotificationDedupe(nowMillis: Long) {
        runCompletionNotificationDedupedAt = runCompletionNotificationDedupedAt
            .filterValues { timestamp -> nowMillis - timestamp <= 60_000L }
            .toMutableMap()
    }

    private fun collectDescendantThreadIDs(parentId: String): List<String> {
        val queue = ArrayDeque(listOf(parentId))
        val visited = mutableSetOf<String>()
        val descendants = mutableListOf<String>()

        while (queue.isNotEmpty()) {
            val currentId = queue.removeFirst()
            for (thread in _threads.value) {
                if (thread.parentThreadId == currentId && visited.add(thread.id)) {
                    descendants.add(thread.id)
                    queue.add(thread.id)
                }
            }
        }

        return descendants
    }

    private fun collectSubtreeThreadIDs(rootId: String): List<String> =
        listOf(rootId) + collectDescendantThreadIDs(rootId)

    private fun setThreadArchivedLocally(threadId: String, isArchived: Boolean) {
        clearRunningState(threadId)
        _readyThreadIDs.value = _readyThreadIDs.value - threadId
        _failedThreadIDs.value = _failedThreadIDs.value - threadId
        activeTurnIdByThread.remove(threadId)

        val current = _threads.value.toMutableList()
        val idx = current.indexOfFirst { it.id == threadId }
        if (idx >= 0) {
            current[idx] = current[idx].copy(
                syncState = if (isArchived) CodexThreadSyncState.ARCHIVED_LOCAL else CodexThreadSyncState.LIVE
            )
            _threads.value = sortThreads(current)
        }

        if (isArchived) {
            addLocallyArchivedThreadID(threadId)
            removeLocallyDeletedThreadID(threadId)
        } else {
            removeLocallyArchivedThreadID(threadId)
        }

        if (_activeThreadId.value == threadId && isArchived) {
            _activeThreadId.value = null
            persistActiveThreadId(null)
        }
    }

    private fun removeThreadLocally(threadId: String, persistAsDeleted: Boolean) {
        clearRunningState(threadId)
        _readyThreadIDs.value = _readyThreadIDs.value - threadId
        _failedThreadIDs.value = _failedThreadIDs.value - threadId
        activeTurnIdByThread.remove(threadId)
        _messagesByThread.value = _messagesByThread.value.toMutableMap().apply {
            remove(threadId)
        }
        _threads.value = _threads.value.filterNot { it.id == threadId }

        if (_activeThreadId.value == threadId) {
            _activeThreadId.value = null
            persistActiveThreadId(null)
        }

        removeLocallyArchivedThreadID(threadId)
        removePersistedThreadRename(threadId)
        removePersistedForkOrigin(threadId)
        removePersistedThreadProjectPath(threadId)
        if (persistAsDeleted) {
            addLocallyDeletedThreadID(threadId)
        } else {
            removeLocallyDeletedThreadID(threadId)
        }
    }

    private fun readPersistedStringSet(key: String): Set<String> {
        val raw = secureStore.readString(key) ?: return emptySet()
        return try {
            json.decodeFromString(ListSerializer(String.serializer()), raw).toSet()
        } catch (_: Exception) {
            emptySet()
        }
    }

    private fun writePersistedStringSet(key: String, ids: Set<String>) {
        secureStore.writeString(
            key,
            json.encodeToString(ListSerializer(String.serializer()), ids.toList().sorted())
        )
    }

    private fun readPersistedThreadRenameMap(): Map<String, String> {
        val raw = secureStore.readString(SecureStore.RENAMED_THREAD_NAMES) ?: return emptyMap()
        return try {
            json.decodeFromString(MapSerializer(String.serializer(), String.serializer()), raw)
        } catch (_: Exception) {
            emptyMap()
        }
    }

    private fun readPersistedForkOriginMap(): Map<String, String> {
        val raw = secureStore.readString(SecureStore.FORKED_THREAD_ORIGINS) ?: return emptyMap()
        return try {
            json.decodeFromString(MapSerializer(String.serializer(), String.serializer()), raw)
        } catch (_: Exception) {
            emptyMap()
        }
    }

    private fun readPersistedThreadProjectMap(): Map<String, String> {
        val raw = secureStore.readString(SecureStore.THREAD_PROJECT_BINDINGS) ?: return emptyMap()
        return try {
            json.decodeFromString(MapSerializer(String.serializer(), String.serializer()), raw)
        } catch (_: Exception) {
            emptyMap()
        }
    }

    private fun persistedActiveThreadId(): String? =
        secureStore.readString(SecureStore.LAST_ACTIVE_THREAD_ID)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }

    private fun writePersistedThreadRenameMap(value: Map<String, String>) {
        secureStore.writeString(
            SecureStore.RENAMED_THREAD_NAMES,
            json.encodeToString(MapSerializer(String.serializer(), String.serializer()), value)
        )
    }

    private fun writePersistedForkOriginMap(value: Map<String, String>) {
        secureStore.writeString(
            SecureStore.FORKED_THREAD_ORIGINS,
            json.encodeToString(MapSerializer(String.serializer(), String.serializer()), value)
        )
    }

    private fun writePersistedThreadProjectMap(value: Map<String, String>) {
        secureStore.writeString(
            SecureStore.THREAD_PROJECT_BINDINGS,
            json.encodeToString(MapSerializer(String.serializer(), String.serializer()), value)
        )
    }

    private fun persistActiveThreadId(threadId: String?) {
        threadId?.trim()?.takeIf { it.isNotEmpty() }?.let {
            secureStore.writeStringSync(SecureStore.LAST_ACTIVE_THREAD_ID, it)
        } ?: secureStore.deleteValueSync(SecureStore.LAST_ACTIVE_THREAD_ID)
    }

    private fun locallyArchivedThreadIDs(): Set<String> =
        readPersistedStringSet(SecureStore.LOCALLY_ARCHIVED_THREAD_IDS)

    private fun locallyDeletedThreadIDs(): Set<String> =
        readPersistedStringSet(SecureStore.LOCALLY_DELETED_THREAD_IDS)

    private fun addLocallyArchivedThreadID(threadId: String) {
        val ids = locallyArchivedThreadIDs().toMutableSet()
        ids.add(threadId)
        writePersistedStringSet(SecureStore.LOCALLY_ARCHIVED_THREAD_IDS, ids)
    }

    private fun removeLocallyArchivedThreadID(threadId: String) {
        val ids = locallyArchivedThreadIDs().toMutableSet()
        ids.remove(threadId)
        writePersistedStringSet(SecureStore.LOCALLY_ARCHIVED_THREAD_IDS, ids)
    }

    private fun addLocallyDeletedThreadID(threadId: String) {
        val ids = locallyDeletedThreadIDs().toMutableSet()
        ids.add(threadId)
        writePersistedStringSet(SecureStore.LOCALLY_DELETED_THREAD_IDS, ids)
    }

    private fun removeLocallyDeletedThreadID(threadId: String) {
        val ids = locallyDeletedThreadIDs().toMutableSet()
        ids.remove(threadId)
        writePersistedStringSet(SecureStore.LOCALLY_DELETED_THREAD_IDS, ids)
    }

    private fun persistThreadRename(name: String?, threadId: String) {
        val trimmedName = name?.trim().orEmpty()
        val normalizedThreadId = threadId.trim()
        if (normalizedThreadId.isEmpty()) return

        val persisted = readPersistedThreadRenameMap().toMutableMap()
        if (trimmedName.isEmpty()) {
            persisted.remove(normalizedThreadId)
        } else {
            persisted[normalizedThreadId] = trimmedName
        }
        writePersistedThreadRenameMap(persisted)
    }

    private fun removePersistedThreadRename(threadId: String) {
        persistThreadRename(null, threadId)
    }

    private fun persistedThreadRename(threadId: String): String? =
        readPersistedThreadRenameMap()[threadId.trim()]?.trim()?.takeIf { it.isNotEmpty() }

    private fun persistThreadProjectPath(projectPath: String?, threadId: String) {
        val normalizedThreadId = threadId.trim().takeIf { it.isNotEmpty() } ?: return
        val normalizedProjectPath = CodexThread.normalizeProjectPath(projectPath)
        val persisted = readPersistedThreadProjectMap().toMutableMap()
        if (normalizedProjectPath == null) {
            persisted.remove(normalizedThreadId)
        } else {
            persisted[normalizedThreadId] = normalizedProjectPath
        }
        writePersistedThreadProjectMap(persisted)
    }

    private fun removePersistedThreadProjectPath(threadId: String) {
        persistThreadProjectPath(null, threadId)
    }

    private fun persistedThreadProjectPath(threadId: String): String? =
        readPersistedThreadProjectMap()[threadId.trim()]?.trim()?.takeIf { it.isNotEmpty() }

    private fun rememberForkOriginIfNeeded(sourceThreadId: String?, forkedThreadId: String) {
        val normalizedSourceThreadId = sourceThreadId?.trim()?.takeIf { it.isNotEmpty() } ?: return
        val normalizedForkedThreadId = forkedThreadId.trim().takeIf { it.isNotEmpty() } ?: return

        val persisted = readPersistedForkOriginMap().toMutableMap()
        if (persisted[normalizedForkedThreadId] == normalizedSourceThreadId) {
            return
        }

        persisted[normalizedForkedThreadId] = normalizedSourceThreadId
        writePersistedForkOriginMap(persisted)
    }

    private fun removePersistedForkOrigin(threadId: String) {
        val normalizedThreadId = threadId.trim().takeIf { it.isNotEmpty() } ?: return
        val persisted = readPersistedForkOriginMap().toMutableMap()
        if (persisted.remove(normalizedThreadId) != null) {
            writePersistedForkOriginMap(persisted)
        }
    }

    private fun persistedForkOrigin(threadId: String): String? =
        readPersistedForkOriginMap()[threadId.trim()]?.trim()?.takeIf { it.isNotEmpty() }

    private fun applyPersistedThreadRename(thread: CodexThread): CodexThread {
        val persistedName = persistedThreadRename(thread.id) ?: return thread
        return thread.copy(title = persistedName, name = persistedName)
    }

    private fun applyPersistedThreadProjectPath(thread: CodexThread): CodexThread {
        val persistedProjectPath = persistedThreadProjectPath(thread.id) ?: return thread
        if (thread.projectKey == persistedProjectPath) {
            return thread
        }
        return thread.copy(cwd = persistedProjectPath)
    }

    private suspend fun handleThreadForkResponse(
        response: RpcMessage,
        sourceThreadId: String,
        sourceProjectPath: String?,
        fallbackProjectPath: String?,
        preferredModelIdentifier: String?,
        usesPostForkResumeOverrides: Boolean
    ): CodexThread {
        val resultObject = response.result?.objectValue
            ?: throw IllegalStateException("thread/fork response missing payload")
        val threadValue = resultObject["thread"]
            ?: throw IllegalStateException("thread/fork response missing thread")
        val decodedThread = CodexThread.fromJson(json, threadValue.toJsonElement())
            ?: throw IllegalStateException("thread/fork response contained an invalid thread")
        val responseProjectPath = resultObject["cwd"]?.stringValue

        val now = System.currentTimeMillis() / 1000.0
        val authoritativeProjectPath = resolveForkProjectPath(
            sourceProjectPath = sourceProjectPath,
            fallbackProjectPath = fallbackProjectPath,
            candidateProjectPaths = listOf(decodedThread.cwd, responseProjectPath),
            forceFallback = usesPostForkResumeOverrides
        )
        val resolvedThread = decodedThread.copy(
            cwd = authoritativeProjectPath,
            createdAtRaw = decodedThread.createdAtRaw ?: now.toString(),
            updatedAtRaw = decodedThread.updatedAtRaw ?: now.toString(),
            forkedFromThreadId = decodedThread.forkedFromThreadId ?: sourceThreadId
        )

        rememberForkOriginIfNeeded(
            sourceThreadId = resolvedThread.forkedFromThreadId,
            forkedThreadId = resolvedThread.id
        )
        upsertThread(resolvedThread)
        selectThread(resolvedThread.id)
        return runCatching {
            ensureThreadResumed(
                threadId = resolvedThread.id,
                preferredProjectPath = if (usesPostForkResumeOverrides) fallbackProjectPath else null,
                modelIdentifierOverride = if (usesPostForkResumeOverrides) preferredModelIdentifier else null,
                sourceProjectPath = sourceProjectPath,
                authoritativeForkProjectPath = authoritativeProjectPath ?: fallbackProjectPath
            )
        }.getOrNull() ?: thread(resolvedThread.id) ?: resolvedThread
    }

    private suspend fun ensureThreadResumed(
        threadId: String,
        preferredProjectPath: String? = null,
        modelIdentifierOverride: String? = null,
        sourceProjectPath: String? = null,
        authoritativeForkProjectPath: String? = null
    ): CodexThread? {
        val normalizedThreadId = threadId.trim().takeIf { it.isNotEmpty() } ?: return null
        val resolvedProjectPath = CodexThread.normalizeProjectPath(preferredProjectPath)
            ?: thread(normalizedThreadId)?.gitWorkingDirectory
        val params = mutableMapOf<String, JsonValue>(
            "threadId" to JsonValue.string(normalizedThreadId)
        )
        resolvedProjectPath?.let { params["cwd"] = JsonValue.string(it) }
        modelIdentifierOverride?.trim()?.takeIf { it.isNotEmpty() }?.let {
            params["model"] = JsonValue.string(it)
        }

        val response = sendRequestWithSandboxFallback(
            method = "thread/resume",
            baseParams = params
        )
        val resultObject = response.result?.objectValue
        val historyMessages = historyDecoder.decodeMessagesFromThreadRead(normalizedThreadId, response.result)
        if (historyMessages.isNotEmpty()) {
            setThreadMessages(
                normalizedThreadId,
                mergeHistoryMessages(normalizedThreadId, historyMessages)
            )
        }

        val existingThread = thread(normalizedThreadId)
        val resumedThread = resultObject
            ?.get("thread")
            ?.let { CodexThread.fromJson(json, it.toJsonElement()) }
        val resumedProjectPath = resolveForkProjectPath(
            sourceProjectPath = sourceProjectPath,
            fallbackProjectPath = authoritativeForkProjectPath ?: resolvedProjectPath,
            candidateProjectPaths = listOf(
                resumedThread?.cwd,
                resultObject?.get("cwd")?.stringValue
            )
        )
        val resolvedThread = when {
            resumedThread != null -> {
                resumedThread.copy(
                    cwd = resumedProjectPath ?: resumedThread.cwd ?: resolvedProjectPath ?: existingThread?.cwd,
                    createdAtRaw = resumedThread.createdAtRaw ?: existingThread?.createdAtRaw,
                    updatedAtRaw = resumedThread.updatedAtRaw ?: existingThread?.updatedAtRaw,
                    forkedFromThreadId = resumedThread.forkedFromThreadId ?: existingThread?.forkedFromThreadId
                )
            }
            existingThread != null && resolvedProjectPath != null -> existingThread.copy(cwd = resolvedProjectPath)
            else -> existingThread
        }

        resolvedThread?.let(::upsertThread)
        selectThread(normalizedThreadId)
        return thread(normalizedThreadId) ?: resolvedThread
    }

    private fun resolveForkProjectPath(
        sourceProjectPath: String?,
        fallbackProjectPath: String?,
        candidateProjectPaths: List<String?>,
        forceFallback: Boolean = false
    ): String? {
        val normalizedFallback = CodexThread.normalizeProjectPath(fallbackProjectPath)
        if (forceFallback) {
            return normalizedFallback
        }

        val normalizedSource = CodexThread.normalizeProjectPath(sourceProjectPath)
        val normalizedCandidates = candidateProjectPaths
            .mapNotNull(CodexThread::normalizeProjectPath)
            .distinct()

        if (normalizedFallback == null) {
            return normalizedCandidates.firstOrNull()
        }
        if (normalizedCandidates.isEmpty()) {
            return normalizedFallback
        }
        if (normalizedCandidates.any { it == normalizedFallback }) {
            return normalizedFallback
        }

        val firstCandidate = normalizedCandidates.first()
        if (shouldPreferForkFallbackProjectPath(
                sourceProjectPath = normalizedSource,
                runtimeProjectPath = firstCandidate,
                fallbackProjectPath = normalizedFallback
            )
        ) {
            return normalizedFallback
        }

        return firstCandidate
    }

    private fun shouldPreferForkFallbackProjectPath(
        sourceProjectPath: String?,
        runtimeProjectPath: String?,
        fallbackProjectPath: String?
    ): Boolean {
        val normalizedSource = CodexThread.normalizeProjectPath(sourceProjectPath)
        val normalizedRuntime = CodexThread.normalizeProjectPath(runtimeProjectPath)
        val normalizedFallback = CodexThread.normalizeProjectPath(fallbackProjectPath)

        if (normalizedRuntime == null || normalizedFallback == null || normalizedRuntime == normalizedFallback) {
            return false
        }

        if (normalizedSource != null
            && normalizedSource != normalizedFallback
            && normalizedRuntime == normalizedSource
        ) {
            return true
        }

        val fallbackIsManagedWorktree = CodexThread.isManagedWorktreeProjectPath(normalizedFallback)
        val runtimeIsManagedWorktree = CodexThread.isManagedWorktreeProjectPath(normalizedRuntime)
        return fallbackIsManagedWorktree && !runtimeIsManagedWorktree
    }

    private fun shouldRetryThreadForkWithoutOverrides(error: Exception): Boolean {
        val rpcError = error as? RpcRequestException ?: return false
        if (rpcError.code != -32600 && rpcError.code != -32602 && rpcError.code != -32000) {
            return false
        }

        val message = rpcError.rpcMessage.lowercase()
        val mentionsUnknownField = message.contains("unknown field")
            || message.contains("unexpected field")
            || message.contains("unrecognized field")
        val mentionsInvalidNamedField = (message.contains("invalid param") || message.contains("invalid params"))
            && (message.contains("field") || message.contains("parameter") || message.contains("param"))
        val mentionsForkOverride = message.contains("cwd")
            || message.contains("modelprovider")
            || message.contains("model provider")
            || message.contains("model")
            || message.contains("sandbox")

        return (mentionsUnknownField || mentionsInvalidNamedField) && mentionsForkOverride
    }

    private fun shouldTreatAsUnsupportedThreadFork(error: Exception): Boolean {
        val rpcError = error as? RpcRequestException ?: return false
        if (rpcError.code == -32601) {
            return true
        }

        val message = rpcError.rpcMessage.lowercase()
        val mentionsUnsupportedMethod = message.contains("method not found")
            || message.contains("unknown method")
            || message.contains("not implemented")
            || message.contains("does not support")
        val mentionsForkSpecificUnsupported = (message.contains("thread/fork") || message.contains("thread fork"))
            && (message.contains("unsupported") || message.contains("not supported"))

        if (rpcError.code != -32600 && rpcError.code != -32602 && rpcError.code != -32000) {
            return mentionsUnsupportedMethod || mentionsForkSpecificUnsupported
        }

        return mentionsUnsupportedMethod || mentionsForkSpecificUnsupported
    }

    private fun shouldRetryWithoutServiceTier(error: Exception): Boolean {
        val rpcError = error as? RpcRequestException ?: return false
        if (rpcError.code != -32600 && rpcError.code != -32602) {
            return false
        }

        val message = rpcError.rpcMessage.lowercase()
        return message.contains("servicetier")
            || message.contains("service tier")
            || message.contains("unknown field")
            || message.contains("unexpected field")
            || message.contains("unrecognized field")
            || message.contains("invalid param")
            || message.contains("invalid params")
    }

    private fun restoreAIChangeSetLedger(snapshot: AIChangeSetLedgerSnapshot) {
        aiChangeSetsById.clear()
        var sanitizedSnapshot = false
        snapshot.changeSets.forEach { changeSet ->
            val sanitizedChangeSet = sanitizeRestoredAIChangeSet(changeSet)
            if (sanitizedChangeSet == null) {
                sanitizedSnapshot = true
                return@forEach
            }
            if (sanitizedChangeSet != changeSet) {
                sanitizedSnapshot = true
            }
            aiChangeSetsById[sanitizedChangeSet.id] = sanitizedChangeSet
        }
        aiChangeSetIdByAssistantMessageId.clear()
        snapshot.changeSetIdByAssistantMessageId.forEach { (assistantMessageId, changeSetId) ->
            if (aiChangeSetsById.containsKey(changeSetId)) {
                aiChangeSetIdByAssistantMessageId[assistantMessageId] = changeSetId
            } else {
                sanitizedSnapshot = true
            }
        }
        aiChangeSetIdByTurnId.clear()
        snapshot.changeSetIdByTurnId.forEach { (turnId, changeSetId) ->
            if (aiChangeSetsById.containsKey(changeSetId)) {
                aiChangeSetIdByTurnId[turnId] = changeSetId
            } else {
                sanitizedSnapshot = true
            }
        }
        repoRootByWorkingDirectory.clear()
        repoRootByWorkingDirectory.putAll(snapshot.repoRootByWorkingDirectory)
        if (sanitizedSnapshot) {
            persistAIChangeSetLedger()
        }
        bumpAIChangeSetRevision()
    }

    private fun persistAIChangeSetLedger() {
        secureStore.writeCodable(
            SecureStore.AI_CHANGE_SET_LEDGER,
            AIChangeSetLedgerSnapshot.serializer(),
            AIChangeSetLedgerSnapshot(
                changeSets = aiChangeSetsById.values.sortedByDescending { it.createdAt },
                changeSetIdByAssistantMessageId = aiChangeSetIdByAssistantMessageId.toMap(),
                changeSetIdByTurnId = aiChangeSetIdByTurnId.toMap(),
                repoRootByWorkingDirectory = repoRootByWorkingDirectory.toMap()
            )
        )
    }

    private fun bumpAIChangeSetRevision() {
        _aiChangeSetRevision.value = _aiChangeSetRevision.value + 1
    }

    private fun mutateAIChangeSets(block: () -> Unit) {
        block()
        persistAIChangeSetLedger()
        bumpAIChangeSetRevision()
    }

    private fun aiChangeSetForAssistantMessage(message: CodexMessage): AIChangeSet? {
        val assistantMessageId = normalizedIdentifier(message.id)
        if (assistantMessageId != null) {
            aiChangeSetIdByAssistantMessageId[assistantMessageId]
                ?.let(aiChangeSetsById::get)
                ?.let { return it }
        }
        val turnId = normalizedIdentifier(message.turnId)
        if (turnId != null) {
            aiChangeSetIdByTurnId[turnId]?.let(aiChangeSetsById::get)?.let { return it }
        }
        return null
    }

    private fun noteAssistantMessage(message: CodexMessage) {
        val turnId = normalizedIdentifier(message.turnId) ?: return
        val assistantMessageId = normalizedIdentifier(message.id) ?: return
        val changeSetId = aiChangeSetIdByTurnId[turnId] ?: return
        val current = aiChangeSetsById[changeSetId] ?: return
        mutateAIChangeSets {
            aiChangeSetIdByAssistantMessageId[assistantMessageId] = changeSetId
            aiChangeSetsById[changeSetId] = current.copy(assistantMessageId = assistantMessageId)
        }
    }

    private fun recordTurnDiffChangeSet(
        threadId: String,
        turnId: String,
        patch: String,
        workingDirectory: String?
    ) {
        recordChangeSetPatch(
            threadId = threadId,
            turnId = turnId,
            patch = patch,
            source = AIChangeSetSource.TURN_DIFF,
            workingDirectory = workingDirectory
        )
    }

    private fun recordFallbackFileChangePatch(
        threadId: String,
        turnId: String,
        patch: String,
        workingDirectory: String?
    ) {
        recordChangeSetPatch(
            threadId = threadId,
            turnId = turnId,
            patch = patch,
            source = AIChangeSetSource.FILE_CHANGE_FALLBACK,
            workingDirectory = workingDirectory
        )
    }

    private fun recordChangeSetPatch(
        threadId: String,
        turnId: String,
        patch: String,
        source: AIChangeSetSource,
        workingDirectory: String?
    ) {
        val normalizedTurnId = normalizedIdentifier(turnId) ?: return
        val normalizedPatch = patch.trim().takeIf { it.isNotEmpty() } ?: return
        val analysis = AIUnifiedPatchParser.analyze(normalizedPatch)
        val patchHash = AIUnifiedPatchParser.hash(normalizedPatch)
        val existingId = aiChangeSetIdByTurnId[normalizedTurnId]
        val existing = existingId?.let(aiChangeSetsById::get)

        val effectiveRepoRoot = canonicalRepoIdentifier(workingDirectory)
        val nextStatus = when {
            analysis.fileChanges.isEmpty() || analysis.unsupportedReasons.isNotEmpty() ->
                AIChangeSetStatus.NOT_REVERTABLE
            else -> AIChangeSetStatus.READY
        }

        val nextChangeSet = (existing ?: AIChangeSet(
            id = existingId ?: "$threadId:$normalizedTurnId",
            repoRoot = effectiveRepoRoot,
            threadId = threadId,
            turnId = normalizedTurnId,
            source = source
        )).copy(
            repoRoot = effectiveRepoRoot ?: existing?.repoRoot,
            threadId = threadId,
            turnId = normalizedTurnId,
            source = if (source == AIChangeSetSource.TURN_DIFF) source else existing?.source ?: source,
            forwardUnifiedPatch = normalizedPatch,
            patchHash = patchHash,
            fileChanges = analysis.fileChanges,
            unsupportedReasons = analysis.unsupportedReasons,
            status = nextStatus,
            finalizedAt = if (source == AIChangeSetSource.TURN_DIFF || nextStatus != AIChangeSetStatus.COLLECTING) {
                System.currentTimeMillis()
            } else {
                existing?.finalizedAt
            },
            fallbackPatchCount = when {
                source == AIChangeSetSource.TURN_DIFF -> existing?.fallbackPatchCount ?: 0
                existing == null -> 1
                else -> existing.fallbackPatchCount + 1
            }
        )

        mutateAIChangeSets {
            aiChangeSetsById[nextChangeSet.id] = nextChangeSet
            aiChangeSetIdByTurnId[normalizedTurnId] = nextChangeSet.id
        }
    }

    private fun markChangeSetRevertAttempt(changeSetId: String) {
        val current = aiChangeSetsById[changeSetId] ?: return
        mutateAIChangeSets {
            aiChangeSetsById[changeSetId] = current.copy(
                revertMetadata = current.revertMetadata.copy(
                    revertAttemptedAtEpochMs = System.currentTimeMillis(),
                    lastRevertError = null
                )
            )
        }
    }

    private fun markChangeSetReverted(changeSetId: String) {
        val current = aiChangeSetsById[changeSetId] ?: return
        mutateAIChangeSets {
            aiChangeSetsById[changeSetId] = current.copy(
                status = AIChangeSetStatus.REVERTED,
                revertMetadata = current.revertMetadata.copy(
                    revertedAtEpochMs = System.currentTimeMillis(),
                    lastRevertError = null
                )
            )
        }
    }

    private fun recordChangeSetError(changeSetId: String, message: String) {
        val current = aiChangeSetsById[changeSetId] ?: return
        mutateAIChangeSets {
            aiChangeSetsById[changeSetId] = current.copy(
                status = if (current.status == AIChangeSetStatus.REVERTED) {
                    current.status
                } else {
                    AIChangeSetStatus.FAILED
                },
                revertMetadata = current.revertMetadata.copy(lastRevertError = message)
            )
        }
    }

    private fun recoverAIChangeSetsFromThreadRead(
        threadId: String,
        params: JsonValue?,
        decodedMessages: List<CodexMessage>
    ) {
        val resultObject = params?.objectValue ?: return
        val threadObject = resultObject["thread"]?.objectValue ?: resultObject
        val turns = threadObject["turns"]?.arrayValue ?: return
        val workingDirectory = thread(threadId)?.gitWorkingDirectory

        turns.forEach { turnValue ->
            val turn = turnValue.objectValue ?: return@forEach
            val turnId = firstStringValue(turn, "id", "turnId") ?: return@forEach
            extractPatchFromJsonValue(turn["diff"])
                ?.takeIf { it.isNotBlank() }
                ?.let {
                    recordTurnDiffChangeSet(
                        threadId = threadId,
                        turnId = turnId,
                        patch = it,
                        workingDirectory = workingDirectory
                    )
                }

            val items = turn["items"]?.arrayValue ?: emptyList()
            items.forEach { itemValue ->
                val item = itemValue.objectValue ?: return@forEach
                extractPatchFromJsonValue(JsonValue.ObjectValue(item))
                    ?.takeIf { it.isNotBlank() }
                    ?.let {
                        recordFallbackFileChangePatch(
                            threadId = threadId,
                            turnId = turnId,
                            patch = it,
                            workingDirectory = workingDirectory
                        )
                    }
            }
        }

        decodedMessages.filter { it.isAssistant }.forEach(::noteAssistantMessage)
    }

    private fun normalizedIdentifier(value: String?): String? =
        value?.trim()?.takeIf { it.isNotEmpty() }

    private fun normalizedInterruptTurnStatus(turnObject: Map<String, JsonValue>): String? {
        val status = firstStringValue(turnObject, "status", "turnStatus", "turn_status") ?: return null
        return status
            .replace("_", "")
            .replace("-", "")
            .lowercase()
    }

    private fun decodeInterruptibleTurnSnapshot(payload: JsonValue?): InterruptibleTurnSnapshot {
        val threadObject = payload
            ?.objectValue
            ?.get("thread")
            ?.objectValue
            ?: return InterruptibleTurnSnapshot()
        val turns = threadObject["turns"]?.arrayValue ?: return InterruptibleTurnSnapshot()
        if (turns.isEmpty()) {
            return InterruptibleTurnSnapshot()
        }

        var latestTurnId: String? = null
        var latestTurnStatus: String? = null
        turns.asReversed().forEach { turnValue ->
            val turn = turnValue.objectValue ?: return@forEach
            val turnId = firstStringValue(turn, "id", "turnId", "turn_id")
                ?.let(::normalizedIdentifier)
            val turnStatus = normalizedInterruptTurnStatus(turn)
            if (latestTurnId == null && turnId != null) {
                latestTurnId = turnId
                latestTurnStatus = turnStatus
            }

            if (!isInterruptibleTurnStatus(turnStatus)) {
                return@forEach
            }

            if (turnId != null) {
                return InterruptibleTurnSnapshot(
                    interruptibleTurnId = turnId,
                    latestTurnId = latestTurnId,
                    latestTurnStatus = latestTurnStatus
                )
            }

            return InterruptibleTurnSnapshot(
                hasInterruptibleTurnWithoutId = true,
                latestTurnId = latestTurnId,
                latestTurnStatus = latestTurnStatus
            )
        }

        return InterruptibleTurnSnapshot(
            latestTurnId = latestTurnId,
            latestTurnStatus = latestTurnStatus
        )
    }

    private fun isInterruptibleTurnStatus(normalizedStatus: String?): Boolean {
        if (normalizedStatus == null) {
            return true
        }

        return when {
            normalizedStatus.contains("inprogress")
                || normalizedStatus.contains("running")
                || normalizedStatus.contains("pending")
                || normalizedStatus.contains("started") -> true
            normalizedStatus.contains("complete")
                || normalizedStatus.contains("failed")
                || normalizedStatus.contains("error")
                || normalizedStatus.contains("interrupt")
                || normalizedStatus.contains("cancel")
                || normalizedStatus.contains("stopped") -> false
            else -> true
        }
    }

    private fun terminalRunCompletionResult(normalizedStatus: String?): CodexRunCompletionResult? {
        if (normalizedStatus == null) {
            return null
        }

        return when {
            normalizedStatus.contains("failed") || normalizedStatus.contains("error") ->
                CodexRunCompletionResult.FAILED
            normalizedStatus.contains("complete")
                || normalizedStatus.contains("success")
                || normalizedStatus.contains("succeed")
                || normalizedStatus.contains("done") ->
                CodexRunCompletionResult.COMPLETED
            normalizedStatus.contains("interrupt")
                || normalizedStatus.contains("cancel")
                || normalizedStatus.contains("stopped") ->
                null
            else -> null
        }
    }

    private fun applyInterruptibleTurnSnapshot(
        threadId: String,
        snapshot: InterruptibleTurnSnapshot,
        allowBackgroundNotification: Boolean
    ) {
        val previousTurnId = activeTurnIdByThread[threadId]
        val wasRunning = isThreadRunning(threadId) || previousTurnId != null
        val hasAuthoritativeNonRunningSnapshot =
            snapshot.latestTurnStatus?.let { !isInterruptibleTurnStatus(it) } == true

        when {
            snapshot.interruptibleTurnId != null -> {
                markThreadRunning(threadId)
                activeTurnIdByThread[threadId] = snapshot.interruptibleTurnId
                return
            }
            snapshot.hasInterruptibleTurnWithoutId -> {
                markThreadRunning(threadId)
                return
            }
        }

        if (wasRunning && !hasAuthoritativeNonRunningSnapshot) {
            return
        }

        clearRunningState(threadId)
        activeTurnIdByThread.remove(threadId)

        when (val completion = terminalRunCompletionResult(snapshot.latestTurnStatus)) {
            CodexRunCompletionResult.COMPLETED -> {
                _readyThreadIDs.value = _readyThreadIDs.value + threadId
                _failedThreadIDs.value = _failedThreadIDs.value - threadId
                if (allowBackgroundNotification && wasRunning) {
                    notifyRunCompletionIfNeeded(
                        threadId = threadId,
                        turnId = snapshot.latestTurnId ?: previousTurnId,
                        result = completion
                    )
                }
            }
            CodexRunCompletionResult.FAILED -> {
                _failedThreadIDs.value = _failedThreadIDs.value + threadId
                _readyThreadIDs.value = _readyThreadIDs.value - threadId
                if (allowBackgroundNotification && wasRunning) {
                    notifyRunCompletionIfNeeded(
                        threadId = threadId,
                        turnId = snapshot.latestTurnId ?: previousTurnId,
                        result = completion
                    )
                }
            }
            null -> Unit
        }
    }

    private fun shouldRetryInterruptWithSnakeCaseParams(error: Exception): Boolean {
        val rpcError = error as? RpcRequestException ?: return false
        if (rpcError.code != -32600 && rpcError.code != -32602) {
            return false
        }

        val message = rpcError.rpcMessage.lowercase()
        val hints = listOf(
            "turnid",
            "threadid",
            "turn_id",
            "thread_id",
            "unknown field",
            "unrecognized field",
            "missing field",
            "invalid"
        )
        return hints.any { message.contains(it) }
    }

    private fun shouldRetryThreadReadTurnSnapshotWithSnakeCase(error: Exception): Boolean {
        val rpcError = error as? RpcRequestException ?: return false
        if (rpcError.code != -32600 && rpcError.code != -32602) {
            return false
        }

        val message = rpcError.rpcMessage.lowercase()
        val hints = listOf(
            "threadid",
            "includeturns",
            "thread_id",
            "include_turns",
            "unknown field",
            "unrecognized field",
            "missing field",
            "invalid"
        )
        return hints.any { message.contains(it) }
    }

    private fun shouldRetryInterruptWithRefreshedTurnId(error: Exception): Boolean {
        val rpcError = error as? RpcRequestException ?: return false
        val message = rpcError.rpcMessage.lowercase()
        val hints = listOf(
            "turn not found",
            "no active turn",
            "not in progress",
            "not running",
            "already completed",
            "already finished",
            "invalid turn",
            "no such turn",
            "not active",
            "does not exist",
            "cannot interrupt"
        )
        return hints.any { message.contains(it) }
    }

    private fun normalizeWorkingDirectory(workingDirectory: String?): String? =
        CodexThread.normalizeProjectPath(workingDirectory)

    private fun rememberRepoRoot(repoRoot: String?, workingDirectory: String?) {
        val normalizedRepoRoot = normalizeWorkingDirectory(repoRoot) ?: return
        val normalizedWorkingDirectory = normalizeWorkingDirectory(workingDirectory) ?: return
        mutateAIChangeSets {
            repoRootByWorkingDirectory[normalizedWorkingDirectory] = normalizedRepoRoot
            aiChangeSetsById.entries.forEach { (id, changeSet) ->
                if (changeSet.repoRoot == null && normalizeWorkingDirectory(changeSet.threadId?.let(::thread)?.gitWorkingDirectory) == normalizedWorkingDirectory) {
                    aiChangeSetsById[id] = changeSet.copy(repoRoot = normalizedRepoRoot)
                }
            }
        }
    }

    private fun canonicalRepoIdentifier(workingDirectory: String?): String? {
        val normalizedWorkingDirectory = normalizeWorkingDirectory(workingDirectory) ?: return null
        return repoRootByWorkingDirectory[normalizedWorkingDirectory] ?: normalizedWorkingDirectory
    }

    private fun hasActiveRunInRepo(workingDirectory: String?): Boolean {
        val repoId = canonicalRepoIdentifier(workingDirectory) ?: return false
        return threads.value.any { candidate ->
            (candidate.id in _runningThreadIDs.value || activeTurnIdByThread[candidate.id] != null) &&
                canonicalRepoIdentifier(candidate.gitWorkingDirectory) == repoId
        }
    }

    private fun overlappingFilesForChangeSet(
        changeSet: AIChangeSet,
        workingDirectory: String?
    ): List<String> {
        val repoId = changeSet.repoRoot ?: canonicalRepoIdentifier(workingDirectory) ?: return emptyList()
        val affectedFiles = changeSet.fileChanges.map { it.path }.toSet()
        if (affectedFiles.isEmpty()) {
            return emptyList()
        }

        val overlaps = mutableSetOf<String>()
        aiChangeSetsById.values.forEach { candidate ->
            if (candidate.id == changeSet.id || candidate.status == AIChangeSetStatus.REVERTED) {
                return@forEach
            }
            if ((candidate.repoRoot ?: canonicalRepoIdentifier(candidate.threadId?.let(::thread)?.gitWorkingDirectory)) != repoId) {
                return@forEach
            }
            overlaps += affectedFiles.intersect(candidate.fileChanges.map { it.path }.toSet())
        }
        return overlaps.toList().sorted()
    }

    private fun envelopeEventObject(payload: Map<String, JsonValue>): Map<String, JsonValue>? =
        payload["msg"]?.objectValue ?: payload["event"]?.objectValue

    private fun extractNotificationThreadId(payload: Map<String, JsonValue>): String? {
        firstStringValue(payload, "threadId", "thread_id", "conversationId", "conversation_id")
            ?.let(::normalizedIdentifier)
            ?.let { return it }
        payload["thread"]?.objectValue?.get("id")?.stringValue
            ?.let(::normalizedIdentifier)
            ?.let { return it }
        firstStringValue(
            payload["turn"]?.objectValue ?: emptyMap(),
            "threadId",
            "thread_id"
        )?.let(::normalizedIdentifier)?.let { return it }
        firstStringValue(
            payload["item"]?.objectValue ?: emptyMap(),
            "threadId",
            "thread_id"
        )?.let(::normalizedIdentifier)?.let { return it }

        val eventObject = envelopeEventObject(payload)
        firstStringValue(
            eventObject ?: emptyMap(),
            "threadId",
            "thread_id",
            "conversationId",
            "conversation_id"
        )?.let(::normalizedIdentifier)?.let { return it }
        eventObject?.get("thread")?.objectValue?.get("id")?.stringValue
            ?.let(::normalizedIdentifier)
            ?.let { return it }
        firstStringValue(
            eventObject?.get("turn")?.objectValue ?: emptyMap(),
            "threadId",
            "thread_id"
        )?.let(::normalizedIdentifier)?.let { return it }
        firstStringValue(
            eventObject?.get("item")?.objectValue ?: emptyMap(),
            "threadId",
            "thread_id"
        )?.let(::normalizedIdentifier)?.let { return it }

        val nestedEventObject = payload["event"]?.objectValue
        firstStringValue(
            nestedEventObject ?: emptyMap(),
            "threadId",
            "thread_id",
            "conversationId",
            "conversation_id"
        )?.let(::normalizedIdentifier)?.let { return it }
        nestedEventObject?.get("thread")?.objectValue?.get("id")?.stringValue
            ?.let(::normalizedIdentifier)
            ?.let { return it }
        firstStringValue(
            nestedEventObject?.get("turn")?.objectValue ?: emptyMap(),
            "threadId",
            "thread_id"
        )?.let(::normalizedIdentifier)?.let { return it }

        return null
    }

    private fun extractNotificationTurnId(payload: Map<String, JsonValue>): String? {
        extractTurnIdentifier(payload["turn"])?.let { return it }
        firstStringValue(payload, "turnId", "turn_id")
            ?.let(::normalizedIdentifier)
            ?.let { return it }
        firstStringValue(
            payload["item"]?.objectValue ?: emptyMap(),
            "turnId",
            "turn_id"
        )?.let(::normalizedIdentifier)?.let { return it }

        val eventObject = envelopeEventObject(payload)
        firstStringValue(
            eventObject ?: emptyMap(),
            "turnId",
            "turn_id"
        )?.let(::normalizedIdentifier)?.let { return it }
        extractTurnIdentifier(eventObject?.get("turn"))?.let { return it }
        firstStringValue(
            eventObject?.get("item")?.objectValue ?: emptyMap(),
            "turnId",
            "turn_id"
        )?.let(::normalizedIdentifier)?.let { return it }

        val nestedEventObject = payload["event"]?.objectValue
        firstStringValue(
            nestedEventObject ?: emptyMap(),
            "turnId",
            "turn_id"
        )?.let(::normalizedIdentifier)?.let { return it }
        extractTurnIdentifier(nestedEventObject?.get("turn"))?.let { return it }

        return null
    }

    private fun extractTurnIdentifier(value: JsonValue?): String? {
        val objectValue = value?.objectValue ?: return null
        return firstStringValue(objectValue, "id", "turnId", "turn_id")
            ?.let(::normalizedIdentifier)
    }

    private fun shouldHandleTurnDiffNotification(
        normalizedMethod: String,
        payload: Map<String, JsonValue>
    ): Boolean {
        if (
            normalizedMethod == "turn/diff/updated"
            || normalizedMethod == "turn_diff_updated"
            || normalizedMethod == "turn_diff"
        ) {
            return true
        }

        if (normalizedMethod == "event") {
            val eventType = firstStringValue(envelopeEventObject(payload) ?: emptyMap(), "type")
                ?.trim()
                ?.lowercase()
                ?.replace('-', '_')
            if (eventType == "turn_diff") {
                return true
            }
        }

        val condensedMethod = normalizedMethod
            .replace("_", "")
            .replace("-", "")
        return condensedMethod.contains("turndiff")
            || normalizedMethod.contains("/diff/")
            || normalizedMethod.startsWith("diff/")
            || normalizedMethod.endsWith("/diff")
            || condensedMethod.contains("itemdiff")
    }

    private fun extractTurnDiffPatch(payload: Map<String, JsonValue>): String? {
        val eventObject = envelopeEventObject(payload)
        val nestedEventObject = payload["event"]?.objectValue
        return firstNonEmptyString(
            firstStringValue(payload, "diff", "unified_diff"),
            firstStringValue(eventObject ?: emptyMap(), "diff", "unified_diff"),
            firstStringValue(nestedEventObject ?: emptyMap(), "diff", "unified_diff")
        )?.takeIf(::looksLikeUnifiedDiffText)
    }

    private fun extractPatchFromJsonValue(value: JsonValue?): String? {
        fun normalizePatchCandidate(candidate: JsonValue?): String? {
            val text = candidate?.stringValue?.trim()?.takeIf { it.isNotEmpty() } ?: return null
            return text.takeIf(::looksLikeUnifiedDiffText)
        }

        fun deepSearch(candidate: JsonValue?): String? {
            when (candidate) {
                is JsonValue.ObjectValue -> {
                    listOf(
                        "diff",
                        "unified_diff",
                        "unifiedDiff",
                        "patch",
                        "forwardPatch",
                        "forward_patch"
                    ).forEach { key ->
                        normalizePatchCandidate(candidate.value[key])?.let { return it }
                    }
                    candidate.value.values.forEach { nested ->
                        if (nested !is JsonValue.ObjectValue && nested !is JsonValue.ArrayValue) {
                            return@forEach
                        }
                        deepSearch(nested)?.let { return it }
                    }
                }
                is JsonValue.ArrayValue -> {
                    candidate.value.forEach { nested ->
                        deepSearch(nested)?.let { return it }
                    }
                }
                else -> Unit
            }
            return null
        }

        return deepSearch(value)
    }

    private fun sanitizeRestoredAIChangeSet(changeSet: AIChangeSet): AIChangeSet? {
        val normalizedPatch = changeSet.forwardUnifiedPatch
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?: return null
        if (!looksLikeUnifiedDiffText(normalizedPatch)) {
            return null
        }

        val analysis = AIUnifiedPatchParser.analyze(normalizedPatch)
        val sanitizedStatus = when (changeSet.status) {
            AIChangeSetStatus.REVERTED -> AIChangeSetStatus.REVERTED
            AIChangeSetStatus.FAILED -> AIChangeSetStatus.FAILED
            AIChangeSetStatus.COLLECTING -> AIChangeSetStatus.COLLECTING
            else -> if (analysis.fileChanges.isEmpty() || analysis.unsupportedReasons.isNotEmpty()) {
                AIChangeSetStatus.NOT_REVERTABLE
            } else {
                AIChangeSetStatus.READY
            }
        }

        return changeSet.copy(
            forwardUnifiedPatch = normalizedPatch,
            patchHash = AIUnifiedPatchParser.hash(normalizedPatch),
            fileChanges = analysis.fileChanges,
            unsupportedReasons = analysis.unsupportedReasons,
            status = sanitizedStatus
        )
    }

    private fun shouldRetrySteerWithRefreshedTurnId(error: Exception): Boolean =
        shouldRetryInterruptWithRefreshedTurnId(error)

    private fun looksLikeUnifiedDiffText(text: String): Boolean {
        val normalized = text.trim()
        if (normalized.isEmpty()) {
            return false
        }
        return normalized.contains("diff --git")
            || (normalized.contains("--- ") && normalized.contains("+++ "))
            || normalized.contains("@@ ")
            || normalized.contains("\n@@")
    }

    private fun workspaceUserMessage(errorCode: String?, rpcMessage: String): String =
        when (errorCode?.trim()) {
            "missing_patch" -> "This response cannot be auto-reverted because no exact patch was captured."
            "missing_working_directory" -> "The selected local folder is not available on this Mac."
            else -> rpcMessage.ifBlank { "Patch revert failed." }
        }

    private fun firstNonEmptyString(vararg values: String?): String? =
        values.firstOrNull { !it.isNullOrBlank() }?.trim()

    private suspend fun sendThreadArchiveRPC(threadId: String, unarchive: Boolean) {
        val method = if (unarchive) "thread/unarchive" else "thread/archive"
        try {
            requireSuccessfulResponse(
                messageTransport.sendRequest(
                    method,
                    JsonValue.obj("threadId" to JsonValue.string(threadId))
                ),
                method
            )
        } catch (e: Exception) {
            Log.w(TAG, "$method RPC failed (non-fatal): ${e.message}")
        }
    }

    private fun getThreadMessages(threadId: String): List<CodexMessage> =
        _messagesByThread.value[threadId] ?: emptyList()

    private fun setThreadMessages(threadId: String, messages: List<CodexMessage>) {
        val sortedMessages = messages.sortedBy { it.orderIndex }
        if (sortedMessages.isNotEmpty()) {
            threadHistoryHydrator.markHydrated(threadId)
        }
        _messagesByThread.value = _messagesByThread.value.toMutableMap().apply {
            this[threadId] = sortedMessages
        }
        messagePersistence.saveLater(_messagesByThread.value)
    }

    private fun appendMessage(threadId: String, message: CodexMessage) {
        val current = getThreadMessages(threadId).toMutableList()
        current.add(message)
        setThreadMessages(threadId, current)
    }

    private fun updateMessageDeliveryState(threadId: String, messageId: String, state: CodexMessageDeliveryState) {
        val current = getThreadMessages(threadId).toMutableList()
        val idx = current.indexOfFirst { it.id == messageId }
        if (idx >= 0) {
            current[idx] = current[idx].copy(deliveryState = state)
            setThreadMessages(threadId, current)
        }
    }

    private fun mergeHistoryMessages(
        threadId: String,
        freshMessages: List<CodexMessage>
    ): List<CodexMessage> {
        val existingMessages = getThreadMessages(threadId)
        if (existingMessages.isEmpty()) {
            return freshMessages
        }

        val activeThreadIDs = activeTurnIdByThread.keys.toSet()
        val runningThreadIDs = _runningThreadIDs.value
        val merged = existingMessages.toMutableList()

        freshMessages.forEach { message ->
            val idx = findHistoryMessageIndex(
                existingMessages = merged,
                incomingMessage = message,
                activeThreadIDs = activeThreadIDs,
                runningThreadIDs = runningThreadIDs
            )
            if (idx >= 0) {
                merged[idx] = reconcileExistingMessage(
                    localMessage = merged[idx],
                    serverMessage = message,
                    activeThreadIDs = activeThreadIDs,
                    runningThreadIDs = runningThreadIDs
                )
            } else {
                merged.add(message)
            }
        }

        return merged.sortedBy { it.orderIndex }
    }

    private fun findHistoryMessageIndex(
        existingMessages: List<CodexMessage>,
        incomingMessage: CodexMessage,
        activeThreadIDs: Set<String>,
        runningThreadIDs: Set<String>
    ): Int {
        val normalizedTurnId = normalizedHistoryIdentifier(incomingMessage.turnId)
        val normalizedItemId = normalizedHistoryIdentifier(incomingMessage.itemId)

        if (normalizedItemId != null) {
            val byItemId = existingMessages.indexOfLast { candidate ->
                candidate.role == incomingMessage.role
                    && candidate.kind == incomingMessage.kind
                    && normalizedHistoryIdentifier(candidate.itemId) == normalizedItemId
            }
            if (byItemId >= 0) return byItemId
        }

        when (incomingMessage.role) {
            CodexMessageRole.ASSISTANT -> {
                if (normalizedTurnId != null) {
                    val byText = existingMessages.indexOfLast { candidate ->
                        candidate.role == CodexMessageRole.ASSISTANT
                            && candidate.turnId == normalizedTurnId
                            && normalizedMessageText(candidate.text) == normalizedMessageText(incomingMessage.text)
                    }
                    if (byText >= 0) return byText

                    val byTurn = existingMessages.indexOfLast { candidate ->
                        candidate.role == CodexMessageRole.ASSISTANT
                            && candidate.turnId == normalizedTurnId
                            && (normalizedHistoryIdentifier(candidate.itemId) == null
                                || normalizedHistoryIdentifier(candidate.itemId) == normalizedItemId)
                    }
                    if (byTurn >= 0) return byTurn

                    if (normalizedItemId == null) {
                        val legacyByTurn = existingMessages.indexOfLast { candidate ->
                            candidate.role == CodexMessageRole.ASSISTANT
                                && candidate.turnId == normalizedTurnId
                                && normalizedHistoryIdentifier(candidate.itemId) == null
                        }
                        if (legacyByTurn >= 0) return legacyByTurn
                    }

                    val isThreadActive = activeThreadIDs.contains(incomingMessage.threadId)
                        || runningThreadIDs.contains(incomingMessage.threadId)
                    if (isThreadActive) {
                        val streamingByTurn = existingMessages.indexOfLast { candidate ->
                            candidate.role == CodexMessageRole.ASSISTANT
                                && candidate.turnId == normalizedTurnId
                                && candidate.isStreaming
                        }
                        if (streamingByTurn >= 0) return streamingByTurn
                    }
                }
            }
            CodexMessageRole.USER -> {
                if (normalizedTurnId != null) {
                    val byConfirmedText = existingMessages.indexOfLast { candidate ->
                        candidate.role == CodexMessageRole.USER
                            && candidate.deliveryState != CodexMessageDeliveryState.FAILED
                            && normalizedMessageText(candidate.text) == normalizedMessageText(incomingMessage.text)
                            && attachmentSignature(candidate.attachments) == attachmentSignature(incomingMessage.attachments)
                            && (candidate.turnId == null || candidate.turnId == normalizedTurnId)
                    }
                    if (byConfirmedText >= 0) return byConfirmedText
                }

                val pendingByText = existingMessages.indexOfLast { candidate ->
                    candidate.role == CodexMessageRole.USER
                        && candidate.deliveryState == CodexMessageDeliveryState.PENDING
                        && normalizedMessageText(candidate.text) == normalizedMessageText(incomingMessage.text)
                        && attachmentSignature(candidate.attachments) == attachmentSignature(incomingMessage.attachments)
                }
                if (pendingByText >= 0) return pendingByText
            }
            CodexMessageRole.SYSTEM -> {
                when (incomingMessage.kind) {
                    CodexMessageKind.THINKING -> {
                        if (normalizedTurnId != null) {
                            val byTurn = existingMessages.indexOfLast { candidate ->
                                candidate.role == CodexMessageRole.SYSTEM
                                    && candidate.kind == CodexMessageKind.THINKING
                                    && candidate.turnId == normalizedTurnId
                            }
                            if (byTurn >= 0) return byTurn
                        }
                    }
                    CodexMessageKind.FILE_CHANGE -> {
                        if (normalizedTurnId != null) {
                            val incomingPaths = normalizedFileChangePathKeys(incomingMessage.text)
                            val candidates = existingMessages.indices.filter { index ->
                                val candidate = existingMessages[index]
                                candidate.role == CodexMessageRole.SYSTEM
                                    && candidate.kind == CodexMessageKind.FILE_CHANGE
                                    && (candidate.turnId == normalizedTurnId || candidate.turnId == null)
                            }
                            if (incomingPaths.isNotEmpty()) {
                                val pathMatch = candidates.lastOrNull { index ->
                                    val candidatePaths = normalizedFileChangePathKeys(existingMessages[index].text)
                                    hasSetIntersection(candidatePaths, incomingPaths)
                                }
                                if (pathMatch != null) return pathMatch
                            }
                            if (candidates.size == 1) {
                                return candidates.last()
                            }
                        }
                    }
                    CodexMessageKind.TOOL_ACTIVITY -> {
                        if (normalizedTurnId != null) {
                            val candidateIndices = existingMessages.indices.filter { index ->
                                val candidate = existingMessages[index]
                                candidate.role == CodexMessageRole.SYSTEM
                                    && candidate.kind == CodexMessageKind.TOOL_ACTIVITY
                                    && candidate.turnId == normalizedTurnId
                            }

                            if (normalizedItemId != null) {
                                val byStableItemId = candidateIndices.lastOrNull { index ->
                                    normalizedHistoryIdentifier(existingMessages[index].itemId) == normalizedItemId
                                }
                                if (byStableItemId != null) return byStableItemId
                            }

                            if (candidateIndices.size == 1) {
                                val onlyCandidate = existingMessages[candidateIndices.last()]
                                if (isProvisionalToolActivityRow(onlyCandidate)
                                    && shouldReconcileToolActivityRow(
                                        localMessage = onlyCandidate,
                                        serverMessage = incomingMessage,
                                        requiresExactText = false
                                    )
                                ) {
                                    return candidateIndices.last()
                                }
                            }

                            if (candidateIndices.size > 1) {
                                val reconcilableIndices = candidateIndices.filter { index ->
                                    shouldReconcileToolActivityRow(
                                        localMessage = existingMessages[index],
                                        serverMessage = incomingMessage,
                                        requiresExactText = true
                                    )
                                }
                                if (reconcilableIndices.size == 1) {
                                    return reconcilableIndices.last()
                                }
                            }
                        }
                    }
                    CodexMessageKind.COMMAND_EXECUTION -> {
                        if (normalizedTurnId != null) {
                            val incomingCommandKey = normalizedCommandExecutionPreviewKey(
                                text = incomingMessage.text,
                                commandDetails = incomingMessage.commandDetails
                            )
                            if (incomingCommandKey != null) {
                                val byCommandKey = existingMessages.indexOfLast { candidate ->
                                    candidate.role == CodexMessageRole.SYSTEM
                                        && candidate.kind == CodexMessageKind.COMMAND_EXECUTION
                                        && candidate.turnId == normalizedTurnId
                                        && normalizedCommandExecutionPreviewKey(
                                            text = candidate.text,
                                            commandDetails = candidate.commandDetails
                                        ) == incomingCommandKey
                                }
                                if (byCommandKey >= 0) return byCommandKey
                            }

                            val candidates = existingMessages.indices.filter { index ->
                                val candidate = existingMessages[index]
                                candidate.role == CodexMessageRole.SYSTEM
                                    && candidate.kind == CodexMessageKind.COMMAND_EXECUTION
                                    && candidate.turnId == normalizedTurnId
                            }
                            if (candidates.size == 1) {
                                return candidates.last()
                            }
                        }
                    }
                    CodexMessageKind.PLAN -> {
                        if (normalizedTurnId != null) {
                            val byTurn = existingMessages.indexOfLast { candidate ->
                                candidate.role == CodexMessageRole.SYSTEM
                                    && candidate.kind == CodexMessageKind.PLAN
                                    && candidate.turnId == normalizedTurnId
                            }
                            if (byTurn >= 0) return byTurn
                        }
                    }
                    CodexMessageKind.SUBAGENT_ACTION -> {
                        if (normalizedTurnId != null) {
                            val byTurnAndText = existingMessages.indexOfLast { candidate ->
                                candidate.role == CodexMessageRole.SYSTEM
                                    && candidate.kind == CodexMessageKind.SUBAGENT_ACTION
                                    && candidate.turnId == normalizedTurnId
                                    && normalizedMessageText(candidate.text) == normalizedMessageText(incomingMessage.text)
                            }
                            if (byTurnAndText >= 0) return byTurnAndText
                        }
                    }
                    CodexMessageKind.USER_INPUT_PROMPT -> {
                        val requestIdKey = incomingMessage.structuredUserInputRequest?.requestIdKey
                        if (requestIdKey != null) {
                            val byRequest = existingMessages.indexOfLast { candidate ->
                                candidate.kind == CodexMessageKind.USER_INPUT_PROMPT
                                    && candidate.structuredUserInputRequest?.requestIdKey == requestIdKey
                            }
                            if (byRequest >= 0) return byRequest
                        }
                    }
                    else -> Unit
                }
            }
        }

        val messageKey = historyMessageKey(incomingMessage)
        val byHistoryKey = existingMessages.indexOfFirst { historyMessageKey(it) == messageKey }
        if (byHistoryKey >= 0) return byHistoryKey

        return -1
    }

    private fun reconcileExistingMessage(
        localMessage: CodexMessage,
        serverMessage: CodexMessage,
        activeThreadIDs: Set<String>,
        runningThreadIDs: Set<String>
    ): CodexMessage {
        val threadIsActive = activeThreadIDs.contains(localMessage.threadId)
            || runningThreadIDs.contains(localMessage.threadId)
        val preservesRunningPresentation = threadIsActive
            && (
                localMessage.turnId == null
                    || serverMessage.turnId == null
                    || localMessage.turnId == serverMessage.turnId
                )
        val localItemId = normalizedHistoryIdentifier(localMessage.itemId)
        val serverItemId = normalizedHistoryIdentifier(serverMessage.itemId)
        val shouldRebindItemId = localItemId == null
            || (
                preservesRunningPresentation
                    && localMessage.role == CodexMessageRole.ASSISTANT
                    && localMessage.isStreaming
                    && serverItemId != null
                    && localItemId != serverItemId
                )
            || (
                localMessage.role == CodexMessageRole.SYSTEM
                    && localMessage.kind == CodexMessageKind.TOOL_ACTIVITY
                    && serverItemId != null
                    && !hasStableToolActivityIdentity(localItemId)
                    && localItemId != serverItemId
                )

        var reconciled = localMessage.copy(
            deliveryState = if (localMessage.deliveryState == CodexMessageDeliveryState.PENDING) {
                CodexMessageDeliveryState.CONFIRMED
            } else {
                localMessage.deliveryState
            },
            turnId = localMessage.turnId ?: serverMessage.turnId,
            itemId = if (shouldRebindItemId) serverItemId ?: localMessage.itemId else localMessage.itemId,
            kind = if (localMessage.kind == CodexMessageKind.CHAT && serverMessage.kind != CodexMessageKind.CHAT) {
                serverMessage.kind
            } else {
                localMessage.kind
            },
            attachments = if (localMessage.attachments.isEmpty()) {
                serverMessage.attachments
            } else {
                localMessage.attachments
            },
            planState = mergePlanState(localMessage.planState, serverMessage.planState),
            subagentAction = serverMessage.subagentAction ?: localMessage.subagentAction,
            structuredUserInputRequest = serverMessage.structuredUserInputRequest
                ?: localMessage.structuredUserInputRequest,
            commandDetails = mergeCommandExecutionDetails(
                localMessage.commandDetails,
                serverMessage.commandDetails
            )
        )

        when (localMessage.role) {
            CodexMessageRole.ASSISTANT -> {
                val serverText = normalizedMessageText(serverMessage.text)
                if (serverText.isNotEmpty()) {
                    reconciled = reconciled.copy(
                        text = if (preservesRunningPresentation) {
                            mergeStreamingSnapshotText(
                                existingText = localMessage.text,
                                incomingText = serverMessage.text
                            )
                        } else {
                            serverMessage.text
                        }
                    )
                }
                reconciled = reconciled.copy(
                    isStreaming = if (preservesRunningPresentation) {
                        localMessage.isStreaming
                            || serverMessage.isStreaming
                            || runningThreadIDs.contains(localMessage.threadId)
                    } else {
                        false
                    }
                )
            }
            CodexMessageRole.SYSTEM -> {
                val serverText = normalizedMessageText(serverMessage.text)
                if (serverText.isNotEmpty()) {
                    reconciled = reconciled.copy(
                        text = if (preservesRunningPresentation && localMessage.isStreaming) {
                            mergeStreamingSnapshotText(
                                existingText = localMessage.text,
                                incomingText = serverMessage.text
                            )
                        } else {
                            serverMessage.text
                        }
                    )
                }
                reconciled = reconciled.copy(
                    isStreaming = if (preservesRunningPresentation) {
                        localMessage.isStreaming
                            || serverMessage.isStreaming
                            || runningThreadIDs.contains(localMessage.threadId)
                    } else {
                        false
                    }
                )
            }
            CodexMessageRole.USER -> {
                if (normalizedMessageText(localMessage.text).isEmpty()
                    && normalizedMessageText(serverMessage.text).isNotEmpty()
                ) {
                    reconciled = reconciled.copy(text = serverMessage.text)
                }
            }
        }

        return reconciled
    }

    private fun mergePlanState(
        localPlanState: CodexPlanState?,
        serverPlanState: CodexPlanState?
    ): CodexPlanState? {
        return when {
            localPlanState == null -> serverPlanState
            serverPlanState == null -> localPlanState
            else -> localPlanState.copy(
                explanation = serverPlanState.explanation ?: localPlanState.explanation,
                steps = if (serverPlanState.steps.isEmpty()) localPlanState.steps else serverPlanState.steps
            )
        }
    }

    private fun mergeCommandExecutionDetails(
        localDetails: CommandExecutionDetails?,
        serverDetails: CommandExecutionDetails?
    ): CommandExecutionDetails? {
        return when {
            localDetails == null -> serverDetails
            serverDetails == null -> localDetails
            else -> localDetails.copy(
                fullCommand = serverDetails.fullCommand.ifBlank { localDetails.fullCommand },
                cwd = serverDetails.cwd ?: localDetails.cwd,
                exitCode = serverDetails.exitCode ?: localDetails.exitCode,
                durationMs = serverDetails.durationMs ?: localDetails.durationMs,
                outputTail = mergeStreamingSnapshotText(
                    existingText = localDetails.outputTail,
                    incomingText = serverDetails.outputTail
                )
            )
        }
    }

    private fun historyMessageKey(message: CodexMessage): String {
        val itemId = normalizedHistoryIdentifier(message.itemId)
        if (itemId != null) {
            return "item:${message.role.name}:${message.kind.name}:$itemId"
        }

        return listOf(
            message.role.name,
            message.turnId ?: "no-turn",
            normalizedMessageText(message.text),
            attachmentSignature(message.attachments)
        ).joinToString(separator = "|")
    }

    private fun attachmentSignature(attachments: List<CodexImageAttachment>): String =
        attachments.joinToString(separator = "|") { attachment ->
            attachment.payloadDataURL
                ?: attachment.sourceURL
                ?: attachment.thumbnailBase64JPEG
                ?: ""
        }

    private fun normalizedMessageText(text: String): String =
        text.trim()

    private fun normalizedHistoryIdentifier(value: String?): String? {
        val trimmed = value?.trim().orEmpty()
        return trimmed.ifEmpty { null }
    }

    private fun shouldReconcileToolActivityRow(
        localMessage: CodexMessage,
        serverMessage: CodexMessage,
        requiresExactText: Boolean
    ): Boolean {
        val localItemId = normalizedHistoryIdentifier(localMessage.itemId)
        val serverItemId = normalizedHistoryIdentifier(serverMessage.itemId)
        if (localItemId != null && localItemId == serverItemId) {
            return true
        }

        val localHasStableIdentity = hasStableToolActivityIdentity(localItemId)
        val serverHasStableIdentity = hasStableToolActivityIdentity(serverItemId)
        if (localHasStableIdentity && serverHasStableIdentity) {
            return false
        }

        val localLines = normalizedToolActivityLines(localMessage.text)
        val serverLines = normalizedToolActivityLines(serverMessage.text)
        if (localLines.isEmpty() || serverLines.isEmpty()) {
            return !localHasStableIdentity || !serverHasStableIdentity
        }

        if (localLines == serverLines) {
            return true
        }

        if (requiresExactText) {
            return false
        }

        return listStartsWith(localLines, serverLines) || listStartsWith(serverLines, localLines)
    }

    private fun hasStableToolActivityIdentity(value: String?): Boolean {
        if (value == null) return false
        return !(value.startsWith("synthetic:tool:") || value.startsWith("turn:"))
    }

    private fun isProvisionalToolActivityRow(message: CodexMessage): Boolean {
        val itemId = normalizedHistoryIdentifier(message.itemId)
        if (hasStableToolActivityIdentity(itemId)) {
            return false
        }
        return message.isStreaming || normalizedToolActivityLines(message.text).isEmpty()
    }

    private fun normalizedToolActivityLines(text: String): List<String> =
        normalizedMessageText(text)
            .lineSequence()
            .map { it.trim().lowercase() }
            .filter { it.isNotEmpty() }
            .toList()

    private fun listStartsWith(values: List<String>, prefix: List<String>): Boolean {
        if (prefix.size > values.size) return false
        return prefix.indices.all { index -> values[index] == prefix[index] }
    }

    private fun hasSetIntersection(left: Set<String>, right: Set<String>): Boolean =
        left.any { it in right }

    private fun mergeStreamingSnapshotText(existingText: String, incomingText: String): String {
        if (existingText.isEmpty()) return incomingText
        if (incomingText.isEmpty()) return existingText
        if (incomingText == existingText) return existingText
        if (existingText.endsWith(incomingText)) return existingText
        if (incomingText.length > existingText.length && incomingText.startsWith(existingText)) {
            return incomingText
        }
        if (existingText.length > incomingText.length && existingText.startsWith(incomingText)) {
            return existingText
        }

        val maxOverlap = minOf(existingText.length, incomingText.length)
        for (overlap in maxOverlap downTo 1) {
            if (existingText.takeLast(overlap) == incomingText.take(overlap)) {
                return existingText + incomingText.drop(overlap)
            }
        }

        return incomingText
    }

    private fun normalizedCommandExecutionPreviewKey(
        text: String,
        commandDetails: CommandExecutionDetails? = null
    ): String? {
        val preferredCommand = commandDetails?.fullCommand?.trim().orEmpty()
        val rawValue = if (preferredCommand.isNotEmpty()) preferredCommand else text.trim()
        if (rawValue.isEmpty()) return null

        val statusPrefixes = setOf("running", "completed", "failed", "stopped")
        val tokens = rawValue.split(Regex("\\s+")).filter { it.isNotBlank() }
        if (tokens.isEmpty()) return null

        val commandTokens = if (tokens.first().lowercase() in statusPrefixes) {
            tokens.drop(1)
        } else {
            tokens
        }
        if (commandTokens.isEmpty()) return null

        val normalized = commandTokens.joinToString(separator = " ") { token ->
            token.trim().trim('"', '\'')
        }.trim().lowercase()
        return normalized.ifEmpty { null }
    }

    private fun normalizedFileChangePathKeys(text: String): Set<String> {
        val keys = mutableSetOf<String>()
        text.lineSequence()
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .forEach { line ->
                when {
                    line.startsWith("Path:", ignoreCase = true) -> {
                        normalizeFileChangePath(line.substringAfter(':'))
                            ?.let(keys::add)
                    }
                    ':' in line -> {
                        val prefix = line.substringBefore(':').trim().lowercase()
                        if (prefix in setOf(
                                "modified",
                                "added",
                                "deleted",
                                "renamed",
                                "created",
                                "edited",
                                "moved"
                            )
                        ) {
                            normalizeFileChangePath(line.substringAfter(':').substringBefore(" (+"))
                                ?.let(keys::add)
                        }
                    }
                }
            }
        return keys
    }

    private fun normalizeFileChangePath(rawPath: String?): String? {
        val trimmed = rawPath?.trim().orEmpty()
        if (trimmed.isEmpty()) return null
        return trimmed
            .substringBefore(" (")
            .trim()
            .lowercase()
    }

    private fun buildStructuredUserInputResponse(
        answersByQuestionID: Map<String, List<String>>
    ): JsonValue {
        val answersObject = answersByQuestionID.entries.associate { entry ->
            entry.key to JsonValue.obj(
                "answers" to JsonValue.ArrayValue(
                    entry.value
                        .map { it.trim() }
                        .filter { it.isNotEmpty() }
                        .map(JsonValue::string)
                )
            )
        }
        return JsonValue.ObjectValue(
            mapOf("answers" to JsonValue.ObjectValue(answersObject))
        )
    }

    private fun requestIdKey(requestID: JsonValue): String = when (requestID) {
        is JsonValue.StringValue -> requestID.value
        is JsonValue.IntValue -> requestID.value.toString()
        else -> requestID.toJsonElement().toString()
    }

    private fun removeStructuredUserInputPrompt(requestID: JsonValue, threadIdHint: String?) {
        val targetRequestIdKey = requestIdKey(requestID)
        val targetThreadIds = threadIdHint?.let(::listOf) ?: _messagesByThread.value.keys
        val updated = _messagesByThread.value.toMutableMap()

        targetThreadIds.forEach { threadId ->
            val filtered = (updated[threadId] ?: emptyList()).filterNot { message ->
                message.kind == CodexMessageKind.USER_INPUT_PROMPT
                    && message.structuredUserInputRequest?.requestIdKey == targetRequestIdKey
            }
            updated[threadId] = filtered
        }

        _messagesByThread.value = updated
    }

    private fun isSubagentItemType(itemType: String): Boolean {
        return itemType == "subagentaction"
            || itemType == "subagent_action"
            || itemType == "collabtoolcall"
            || itemType == "collab_tool_call"
            || itemType.startsWith("collabagentspawn")
            || itemType.startsWith("collabwaiting")
            || itemType.startsWith("collabclose")
            || itemType.startsWith("collabresume")
            || itemType.startsWith("collabagentinteraction")
    }

    private fun findSystemMessageIndex(
        messages: List<CodexMessage>,
        kind: CodexMessageKind,
        turnId: String?,
        itemId: String?
    ): Int {
        if (!itemId.isNullOrBlank()) {
            val byItemId = messages.indexOfLast { message ->
                message.kind == kind && message.itemId == itemId
            }
            if (byItemId >= 0) return byItemId
        }

        if (!turnId.isNullOrBlank()) {
            val byTurnId = messages.indexOfLast { message ->
                message.kind == kind && message.turnId == turnId
            }
            if (byTurnId >= 0) return byTurnId
        }

        return messages.indexOfLast { message -> message.kind == kind }
    }

    private fun findUpsertSystemMessageIndex(
        messages: List<CodexMessage>,
        kind: CodexMessageKind,
        turnId: String?,
        itemId: String?,
        text: String,
        commandDetails: CommandExecutionDetails?
    ): Int {
        if (!itemId.isNullOrBlank()) {
            val byItemId = messages.indexOfLast { message ->
                message.kind == kind && message.itemId == itemId
            }
            if (byItemId >= 0) return byItemId
        }

        val resolvedTurnId = turnId?.trim()?.takeIf { it.isNotEmpty() }
        if (resolvedTurnId == null) {
            return when (kind) {
                CodexMessageKind.THINKING,
                CodexMessageKind.PLAN,
                CodexMessageKind.SUBAGENT_ACTION,
                CodexMessageKind.USER_INPUT_PROMPT -> findSystemMessageIndex(messages, kind, turnId, itemId)
                else -> -1
            }
        }

        return when (kind) {
            CodexMessageKind.COMMAND_EXECUTION -> {
                val incomingCommandKey = normalizedCommandExecutionPreviewKey(text, commandDetails)
                val byCommandKey = if (incomingCommandKey != null) {
                    messages.indexOfLast { message ->
                        message.kind == CodexMessageKind.COMMAND_EXECUTION
                            && message.turnId == resolvedTurnId
                            && normalizedCommandExecutionPreviewKey(
                                text = message.text,
                                commandDetails = message.commandDetails
                            ) == incomingCommandKey
                    }
                } else {
                    -1
                }
                if (byCommandKey >= 0) byCommandKey else -1
            }
            CodexMessageKind.FILE_CHANGE -> {
                val incomingPaths = normalizedFileChangePathKeys(text)
                val candidates = messages.indices.filter { index ->
                    val candidate = messages[index]
                    candidate.kind == CodexMessageKind.FILE_CHANGE
                        && (candidate.turnId == resolvedTurnId || candidate.turnId == null)
                }
                when {
                    incomingPaths.isNotEmpty() -> candidates.lastOrNull { index ->
                        val candidatePaths = normalizedFileChangePathKeys(messages[index].text)
                        hasSetIntersection(candidatePaths, incomingPaths)
                    } ?: -1
                    candidates.size == 1 -> candidates.last()
                    else -> -1
                }
            }
            CodexMessageKind.TOOL_ACTIVITY -> {
                val candidateIndices = messages.indices.filter { index ->
                    val candidate = messages[index]
                    candidate.kind == CodexMessageKind.TOOL_ACTIVITY
                        && candidate.turnId == resolvedTurnId
                }
                if (candidateIndices.isEmpty()) {
                    -1
                } else {
                    val incomingMessage = CodexMessage(
                        threadId = "",
                        role = CodexMessageRole.SYSTEM,
                        kind = CodexMessageKind.TOOL_ACTIVITY,
                        text = text,
                        turnId = resolvedTurnId,
                        itemId = itemId,
                        commandDetails = commandDetails
                    )
                    val exactMatches = candidateIndices.filter { index ->
                        shouldReconcileToolActivityRow(
                            localMessage = messages[index],
                            serverMessage = incomingMessage,
                            requiresExactText = true
                        )
                    }
                    when {
                        exactMatches.size == 1 -> exactMatches.last()
                        candidateIndices.size == 1 && isProvisionalToolActivityRow(messages[candidateIndices.last()]) &&
                            shouldReconcileToolActivityRow(
                                localMessage = messages[candidateIndices.last()],
                                serverMessage = incomingMessage,
                                requiresExactText = false
                            ) -> candidateIndices.last()
                        else -> -1
                    }
                }
            }
            CodexMessageKind.THINKING,
            CodexMessageKind.PLAN,
            CodexMessageKind.SUBAGENT_ACTION -> findSystemMessageIndex(messages, kind, turnId, itemId)
            else -> -1
        }
    }

    private fun upsertSystemMessage(
        threadId: String,
        turnId: String?,
        itemId: String?,
        kind: CodexMessageKind,
        text: String,
        isStreaming: Boolean,
        messages: MutableList<CodexMessage>,
        appendDuringStreaming: Boolean = false,
        commandDetails: CommandExecutionDetails? = null
    ) {
        val resolvedText = if (kind == CodexMessageKind.COMMAND_EXECUTION) {
            text.trimEnd()
        } else {
            text.trim()
        }
        if (resolvedText.isEmpty() && commandDetails == null) {
            return
        }

        val idx = findUpsertSystemMessageIndex(
            messages = messages,
            kind = kind,
            turnId = turnId,
            itemId = itemId,
            text = resolvedText,
            commandDetails = commandDetails
        )
        if (idx >= 0) {
            val existing = messages[idx]
            val mergedText = when {
                appendDuringStreaming && isStreaming && existing.isStreaming ->
                    mergeSystemDeltaText(existing.text, resolvedText, kind)
                resolvedText.isNotEmpty() -> resolvedText
                else -> existing.text
            }
            messages[idx] = existing.copy(
                text = mergedText,
                isStreaming = isStreaming,
                turnId = turnId ?: existing.turnId,
                itemId = itemId ?: existing.itemId,
                commandDetails = commandDetails ?: existing.commandDetails
            )
            return
        }

        messages.add(
            CodexMessage(
                threadId = threadId,
                role = CodexMessageRole.SYSTEM,
                kind = kind,
                text = resolvedText,
                turnId = turnId,
                itemId = itemId,
                isStreaming = isStreaming,
                orderIndex = CodexMessageOrderCounter.next(),
                deliveryState = CodexMessageDeliveryState.CONFIRMED,
                commandDetails = commandDetails
            )
        )
    }

    private fun mergeSystemDeltaText(
        existingText: String,
        incomingText: String,
        kind: CodexMessageKind
    ): String {
        if (incomingText.isBlank()) {
            return existingText
        }
        if (existingText.isBlank()) {
            return incomingText
        }

        return when (kind) {
            CodexMessageKind.TOOL_ACTIVITY -> {
                val mergedLines = existingText
                    .lineSequence()
                    .map(String::trim)
                    .filter { it.isNotEmpty() }
                    .toMutableList()
                incomingText
                    .lineSequence()
                    .map(String::trim)
                    .filter { it.isNotEmpty() }
                    .forEach { line ->
                        if (line !in mergedLines) {
                            mergedLines += line
                        }
                    }
                mergedLines.joinToString("\n")
            }
            else -> existingText + incomingText
        }
    }

    private fun upsertThinkingMessage(
        threadId: String,
        turnId: String?,
        itemId: String?,
        text: String,
        isStreaming: Boolean,
        messages: MutableList<CodexMessage>
    ) {
        val trimmed = text.trim()
        if (trimmed.isEmpty()) return

        val idx = findSystemMessageIndex(messages, CodexMessageKind.THINKING, turnId, itemId)
        if (idx >= 0) {
            val existing = messages[idx]
            val mergedText = if (existing.isStreaming && isStreaming) {
                (existing.text + trimmed).trim()
            } else {
                trimmed
            }
            messages[idx] = existing.copy(
                text = mergedText,
                isStreaming = isStreaming
            )
            return
        }

        messages.add(
            CodexMessage(
                threadId = threadId,
                role = CodexMessageRole.SYSTEM,
                kind = CodexMessageKind.THINKING,
                text = trimmed,
                turnId = turnId,
                itemId = itemId,
                isStreaming = isStreaming,
                orderIndex = CodexMessageOrderCounter.next(),
                deliveryState = CodexMessageDeliveryState.CONFIRMED
            )
        )
    }

    private fun upsertPlanMessage(
        threadId: String,
        turnId: String?,
        itemId: String?,
        text: String,
        planState: CodexPlanState?,
        isStreaming: Boolean,
        messages: MutableList<CodexMessage>
    ) {
        val idx = findSystemMessageIndex(messages, CodexMessageKind.PLAN, turnId, itemId)
        val resolvedText = text.ifBlank {
            planState?.explanation ?: "Planning..."
        }

        if (idx >= 0) {
            val existing = messages[idx]
            val existingPlanState = existing.planState
            val mergedPlanState = when {
                existingPlanState == null -> planState
                planState == null -> existingPlanState
                else -> existingPlanState.copy(
                    explanation = planState.explanation ?: existingPlanState.explanation,
                    steps = if (planState.steps.isEmpty()) existingPlanState.steps else planState.steps
                )
            }
            messages[idx] = existing.copy(
                text = if (isStreaming && existing.isStreaming && text.isNotBlank()) {
                    (existing.text + text).trim()
                } else {
                    resolvedText
                },
                isStreaming = isStreaming,
                planState = mergedPlanState
            )
            return
        }

        messages.add(
            CodexMessage(
                threadId = threadId,
                role = CodexMessageRole.SYSTEM,
                kind = CodexMessageKind.PLAN,
                text = resolvedText,
                turnId = turnId,
                itemId = itemId,
                isStreaming = isStreaming,
                orderIndex = CodexMessageOrderCounter.next(),
                deliveryState = CodexMessageDeliveryState.CONFIRMED,
                planState = planState
            )
        )
    }

    private fun upsertSubagentActionMessage(
        threadId: String,
        turnId: String?,
        itemId: String?,
        action: CodexSubagentAction,
        isStreaming: Boolean,
        messages: MutableList<CodexMessage>
    ) {
        val idx = findSystemMessageIndex(messages, CodexMessageKind.SUBAGENT_ACTION, turnId, itemId)
        if (idx >= 0) {
            val existing = messages[idx]
            messages[idx] = existing.copy(
                text = action.summaryText,
                isStreaming = isStreaming,
                subagentAction = action
            )
            return
        }

        messages.add(
            CodexMessage(
                threadId = threadId,
                role = CodexMessageRole.SYSTEM,
                kind = CodexMessageKind.SUBAGENT_ACTION,
                text = action.summaryText,
                turnId = turnId,
                itemId = itemId,
                isStreaming = isStreaming,
                orderIndex = CodexMessageOrderCounter.next(),
                deliveryState = CodexMessageDeliveryState.CONFIRMED,
                subagentAction = action
            )
        )
    }

    private fun upsertStructuredUserInputPromptMessage(
        threadId: String,
        turnId: String?,
        itemId: String?,
        request: CodexStructuredUserInputRequest,
        messages: MutableList<CodexMessage>
    ) {
        val idx = messages.indexOfLast { message ->
            message.kind == CodexMessageKind.USER_INPUT_PROMPT
                && message.structuredUserInputRequest?.requestIdKey == request.requestIdKey
        }

        val summaryText = request.questions.firstOrNull()?.question ?: "Input requested"
        if (idx >= 0) {
            val existing = messages[idx]
            messages[idx] = existing.copy(
                text = summaryText,
                structuredUserInputRequest = request,
                itemId = itemId ?: existing.itemId,
                turnId = turnId ?: existing.turnId
            )
            return
        }

        messages.add(
            CodexMessage(
                threadId = threadId,
                role = CodexMessageRole.SYSTEM,
                kind = CodexMessageKind.USER_INPUT_PROMPT,
                text = summaryText,
                turnId = turnId,
                itemId = itemId,
                orderIndex = CodexMessageOrderCounter.next(),
                deliveryState = CodexMessageDeliveryState.CONFIRMED,
                structuredUserInputRequest = request
            )
        )
    }

    private fun markThreadRunning(threadId: String) {
        _runningThreadIDs.value = _runningThreadIDs.value + threadId
        _readyThreadIDs.value = _readyThreadIDs.value - threadId
        _failedThreadIDs.value = _failedThreadIDs.value - threadId
        syncBackgroundTurnMonitoring()
    }

    private fun clearRunningState(threadId: String) {
        _runningThreadIDs.value = _runningThreadIDs.value - threadId
        mirroredRunningCatchupThreadIDs.remove(threadId)
        syncBackgroundTurnMonitoring()
    }

    private fun threadHasActiveOrRunningTurn(threadId: String): Boolean =
        activeTurnIdByThread.containsKey(threadId) || threadId in _runningThreadIDs.value

    fun markMirroredRunningCatchupNeeded(threadId: String) {
        mirroredRunningCatchupThreadIDs.add(threadId)
    }

    fun clearMirroredRunningCatchupNeeded(threadId: String) {
        mirroredRunningCatchupThreadIDs.remove(threadId)
    }

    private fun shouldPrioritizeMirroredRunningCatchup(threadId: String): Boolean =
        threadId in mirroredRunningCatchupThreadIDs && threadHasActiveOrRunningTurn(threadId)

    private fun syncBackgroundTurnMonitoring() {
        val notificationsEnabled = runCompletionNotifier.canPostNotifications()
        Log.d(
            TAG,
            "syncBackgroundTurnMonitoring foreground=$isAppInForeground running=${_runningThreadIDs.value.size} notificationsEnabled=$notificationsEnabled"
        )
        backgroundTurnMonitor.sync(
            appInForeground = isAppInForeground,
            runningThreadCount = _runningThreadIDs.value.size,
            enabled = notificationsEnabled
        )
    }

    fun isThreadRunning(threadId: String): Boolean =
        threadId in _runningThreadIDs.value
}

data class ApprovalRequest(
    val requestId: JsonValue,
    val toolName: String,
    val description: String,
    val threadId: String? = null
)

private data class InterruptibleTurnSnapshot(
    val interruptibleTurnId: String? = null,
    val hasInterruptibleTurnWithoutId: Boolean = false,
    val latestTurnId: String? = null,
    val latestTurnStatus: String? = null
)
