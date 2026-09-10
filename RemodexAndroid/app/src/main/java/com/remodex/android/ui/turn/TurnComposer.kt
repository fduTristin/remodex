package com.remodex.android.ui.turn

import android.Manifest
import android.content.pm.PackageManager
import android.net.Uri
import android.widget.Toast
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.defaultMinSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.imePadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.ArrowUpward
import androidx.compose.material.icons.filled.Bolt
import androidx.compose.material.icons.filled.Check
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material.icons.filled.Mic
import androidx.compose.material.icons.filled.Psychology
import androidx.compose.material.icons.filled.Stop
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.LocalMinimumInteractiveComponentEnforcement
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.focus.onFocusChanged
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalLifecycleOwner
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.unit.dp
import androidx.core.content.ContextCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import com.remodex.android.data.model.CodexCollaborationModeKind
import com.remodex.android.data.model.CodexFuzzyFileMatch
import com.remodex.android.data.model.CodexImageAttachment
import com.remodex.android.data.model.CodexModelOption
import com.remodex.android.data.model.CodexServiceTier
import com.remodex.android.data.model.CodexSkillMetadata
import com.remodex.android.data.model.CodexTurnSkillMention
import com.remodex.android.service.VoiceRecordingException
import com.remodex.android.service.VoiceRecordingManager
import com.remodex.android.ui.theme.AccentPlan
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

private const val MAX_COMPOSER_ATTACHMENTS = 4
private val ComposerSecondaryGray = Color(0xFF8E8E93)
private val ComposerPlaceholderGray = Color(0xFFC7C7CC)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun TurnComposer(
    draftKey: String?,
    isRunning: Boolean,
    isConnected: Boolean,
    autocompleteRoot: String?,
    availableModels: List<CodexModelOption>,
    selectedModelId: String?,
    selectedModelTitle: String,
    selectedReasoningEffort: String?,
    selectedServiceTier: CodexServiceTier?,
    supportedReasoningEfforts: List<String>,
    onSelectModel: (String?) -> Unit,
    onSelectReasoningEffort: (String?) -> Unit,
    onSelectServiceTier: (CodexServiceTier?) -> Unit,
    onFuzzyFileSearch: suspend (String, List<String>, String?) -> List<CodexFuzzyFileMatch>,
    onListSkills: suspend (List<String>?, Boolean) -> List<CodexSkillMetadata>,
    supportsReviewCommand: Boolean,
    supportsForkCommand: Boolean,
    onReviewAction: (ComposerReviewTarget) -> Unit,
    onForkAction: (ComposerForkDestination) -> Unit,
    onShowStatus: () -> Unit,
    onSteerQueuedDraft: suspend (
        String,
        List<CodexImageAttachment>,
        List<CodexTurnSkillMention>,
        CodexCollaborationModeKind?
    ) -> Unit,
    onTranscribeVoiceClip: suspend (ByteArray, Long) -> String,
    onSend: (
        String,
        List<CodexImageAttachment>,
        List<CodexTurnSkillMention>,
        CodexCollaborationModeKind?
    ) -> Unit,
    onStop: () -> Unit,
    onInputFocusChanged: (Boolean) -> Unit = {},
    modifier: Modifier = Modifier
) {
    val context = LocalContext.current
    val lifecycleOwner = LocalLifecycleOwner.current
    val coroutineScope = rememberCoroutineScope()

    var input by remember(draftKey) { mutableStateOf("") }
    var attachments by remember(draftKey) { mutableStateOf<List<CodexImageAttachment>>(emptyList()) }
    var previewAttachment by remember { mutableStateOf<CodexImageAttachment?>(null) }
    var isAttachmentMenuOpen by remember { mutableStateOf(false) }
    var isModelMenuOpen by remember { mutableStateOf(false) }
    var isRuntimeMenuOpen by remember { mutableStateOf(false) }
    var isImportingAttachments by remember { mutableStateOf(false) }
    var pendingCameraCaptureUri by remember { mutableStateOf<Uri?>(null) }
    var isVoicePreflighting by remember { mutableStateOf(false) }
    var isVoiceRecording by remember { mutableStateOf(false) }
    var isVoiceTranscribing by remember { mutableStateOf(false) }
    var queuedDrafts by remember(draftKey) { mutableStateOf<List<QueuedTurnDraft>>(emptyList()) }
    var queuedDraftPauseMessage by remember(draftKey) { mutableStateOf<String?>(null) }
    var steeringDraftId by remember(draftKey) { mutableStateOf<String?>(null) }
    var isPlanModeArmed by rememberSaveable(draftKey) { mutableStateOf(false) }
    var mentionedFiles by remember(draftKey) { mutableStateOf<List<ComposerMentionedFile>>(emptyList()) }
    var mentionedSkills by remember(draftKey) { mutableStateOf<List<ComposerMentionedSkill>>(emptyList()) }
    var fileAutocompleteItems by remember(draftKey) { mutableStateOf<List<CodexFuzzyFileMatch>>(emptyList()) }
    var isFileAutocompleteVisible by remember(draftKey) { mutableStateOf(false) }
    var isFileAutocompleteLoading by remember(draftKey) { mutableStateOf(false) }
    var fileAutocompleteQuery by remember(draftKey) { mutableStateOf("") }
    var skillAutocompleteItems by remember(draftKey) { mutableStateOf<List<CodexSkillMetadata>>(emptyList()) }
    var isSkillAutocompleteVisible by remember(draftKey) { mutableStateOf(false) }
    var isSkillAutocompleteLoading by remember(draftKey) { mutableStateOf(false) }
    var skillAutocompleteQuery by remember(draftKey) { mutableStateOf("") }
    var slashCommandItems by remember(draftKey) { mutableStateOf<List<ComposerSlashCommand>>(emptyList()) }
    var slashCommandQuery by remember(draftKey) { mutableStateOf("") }
    var slashAutocompleteMode by remember(draftKey) { mutableStateOf(ComposerSlashAutocompleteMode.COMMANDS) }
    val resolvedModelOption = remember(availableModels, selectedModelId) {
        availableModels.firstOrNull { model ->
            model.id == selectedModelId || model.model == selectedModelId
        } ?: availableModels.firstOrNull { it.isDefault } ?: availableModels.firstOrNull()
    }
    val selectedReasoningLabel = remember(selectedReasoningEffort, resolvedModelOption) {
        (selectedReasoningEffort ?: resolvedModelOption?.defaultReasoningEffort ?: "auto")
            .replaceFirstChar { it.uppercase() }
    }
    val voiceRecordingManager = remember(context) { VoiceRecordingManager(context) }
    val voiceAudioLevels by voiceRecordingManager.audioLevels.collectAsState()
    val voiceRecordingDurationMs by voiceRecordingManager.recordingDurationMs.collectAsState()
    val availableSlashCommands = remember(supportsReviewCommand, supportsForkCommand) {
        ComposerSlashCommand.availableCommands(allowsForkCommand = supportsForkCommand)
            .filter { command ->
                when (command) {
                    ComposerSlashCommand.REVIEW -> supportsReviewCommand
                    else -> true
                }
            }
    }

    val isVoiceBusy = isVoicePreflighting || isVoiceRecording || isVoiceTranscribing
    val canSend = (input.isNotBlank() || attachments.isNotEmpty()) && !isImportingAttachments && !isVoiceBusy

    fun currentDraftSnapshot(): QueuedTurnDraft = QueuedTurnDraft(
        text = input.trim(),
        attachments = attachments,
        skillMentions = mentionedSkills.map {
            CodexTurnSkillMention(
                id = it.name.trim().lowercase(),
                name = it.name,
                path = it.path
            )
        },
        collaborationMode = if (isPlanModeArmed) CodexCollaborationModeKind.PLAN else null
    )

    fun clearComposerInputs() {
        input = ""
        attachments = emptyList()
        mentionedFiles = emptyList()
        mentionedSkills = emptyList()
        isPlanModeArmed = false
        fileAutocompleteItems = emptyList()
        fileAutocompleteQuery = ""
        isFileAutocompleteVisible = false
        isFileAutocompleteLoading = false
        skillAutocompleteItems = emptyList()
        skillAutocompleteQuery = ""
        isSkillAutocompleteVisible = false
        isSkillAutocompleteLoading = false
        slashCommandItems = emptyList()
        slashCommandQuery = ""
        slashAutocompleteMode = ComposerSlashAutocompleteMode.COMMANDS
    }

    fun enqueueCurrentDraft() {
        if (!canSend) return
        val snapshot = currentDraftSnapshot()
        queuedDrafts = queuedDrafts + snapshot
        clearComposerInputs()
        Toast.makeText(
            context,
            if (queuedDrafts.size == 1) "Draft queued." else "Draft added to queue.",
            Toast.LENGTH_SHORT
        ).show()
    }

    fun restoreQueuedDraft(draftId: String) {
        val draft = queuedDrafts.firstOrNull { it.id == draftId } ?: return
        queuedDrafts = queuedDrafts.filterNot { it.id == draftId }
        input = draft.text
        attachments = draft.attachments
        mentionedFiles = emptyList()
        mentionedSkills = draft.skillMentions.map { mention ->
            ComposerMentionedSkill(
                name = mention.name ?: mention.id,
                path = mention.path
            )
        }
        isPlanModeArmed = draft.collaborationMode == CodexCollaborationModeKind.PLAN
        fileAutocompleteItems = emptyList()
        fileAutocompleteQuery = ""
        isFileAutocompleteVisible = false
        isFileAutocompleteLoading = false
        skillAutocompleteItems = emptyList()
        skillAutocompleteQuery = ""
        isSkillAutocompleteVisible = false
        isSkillAutocompleteLoading = false
        slashCommandItems = emptyList()
        slashCommandQuery = ""
        slashAutocompleteMode = ComposerSlashAutocompleteMode.COMMANDS
    }
    fun importCapturedImage(uri: Uri) {
        coroutineScope.launch {
            isImportingAttachments = true
            val imported = buildCodexImageAttachment(context, uri)
            if (imported != null) {
                attachments = (attachments + imported).take(MAX_COMPOSER_ATTACHMENTS)
            } else {
                Toast.makeText(context, "Could not attach the captured image.", Toast.LENGTH_SHORT).show()
                deleteCapturedImage(context, uri)
            }
            isImportingAttachments = false
        }
    }

    fun launchCameraCapture(onLaunch: (Uri) -> Unit) {
        if (attachments.size >= MAX_COMPOSER_ATTACHMENTS) {
            Toast.makeText(context, "You can attach up to $MAX_COMPOSER_ATTACHMENTS images.", Toast.LENGTH_SHORT).show()
            isAttachmentMenuOpen = false
            return
        }

        if (!context.packageManager.hasSystemFeature(PackageManager.FEATURE_CAMERA_ANY)) {
            Toast.makeText(context, "Camera capture is unavailable on this device.", Toast.LENGTH_SHORT).show()
            isAttachmentMenuOpen = false
            return
        }

        val captureUri = createCameraCaptureUri(context)
        if (captureUri == null) {
            Toast.makeText(context, "Could not prepare the camera capture.", Toast.LENGTH_SHORT).show()
            isAttachmentMenuOpen = false
            return
        }

        pendingCameraCaptureUri = captureUri
        isAttachmentMenuOpen = false
        onLaunch(captureUri)
    }

    val takePictureLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.TakePicture()
    ) { success ->
        val captureUri = pendingCameraCaptureUri
        pendingCameraCaptureUri = null
        if (!success || captureUri == null) {
            deleteCapturedImage(context, captureUri)
            if (!success) {
                Toast.makeText(context, "Camera capture was cancelled.", Toast.LENGTH_SHORT).show()
            }
            return@rememberLauncherForActivityResult
        }
        importCapturedImage(captureUri)
    }

    val requestCameraPermissionLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.RequestPermission()
    ) { granted ->
        if (granted) {
            launchCameraCapture(takePictureLauncher::launch)
        } else {
            pendingCameraCaptureUri?.let { deleteCapturedImage(context, it) }
            pendingCameraCaptureUri = null
            Toast.makeText(context, "Camera permission is required to take a photo.", Toast.LENGTH_SHORT).show()
        }
    }

    suspend fun cancelVoiceRecording() {
        voiceRecordingManager.cancelRecording()
        isVoicePreflighting = false
        isVoiceRecording = false
        isVoiceTranscribing = false
    }

    suspend fun startVoiceRecording() {
        if (isRunning || isVoiceBusy) {
            return
        }
        if (!isConnected) {
            Toast.makeText(context, "Connect to your Mac before using voice transcription.", Toast.LENGTH_SHORT).show()
            return
        }

        isVoicePreflighting = true
        try {
            voiceRecordingManager.startRecording()
            isVoiceRecording = true
        } finally {
            isVoicePreflighting = false
        }
    }

    suspend fun stopVoiceRecordingAndTranscribe() {
        isVoiceTranscribing = true
        try {
            val clip = voiceRecordingManager.stopRecording()
            isVoiceRecording = false
            if (clip == null) {
                return
            }

            val transcript = onTranscribeVoiceClip(clip.wavData, clip.durationMs).trim()
            if (transcript.isNotEmpty()) {
                input = appendVoiceTranscript(input, transcript)
            }
        } finally {
            isVoicePreflighting = false
            isVoiceRecording = false
            isVoiceTranscribing = false
        }
    }

    val requestMicrophonePermissionLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.RequestPermission()
    ) { granted ->
        if (!granted) {
            isVoicePreflighting = false
            Toast.makeText(context, "Microphone access is required for voice transcription.", Toast.LENGTH_SHORT).show()
            return@rememberLauncherForActivityResult
        }

        coroutineScope.launch {
            try {
                startVoiceRecording()
            } catch (error: VoiceRecordingException) {
                Toast.makeText(context, error.message ?: "Voice recording failed.", Toast.LENGTH_LONG).show()
            } catch (error: Exception) {
                Toast.makeText(context, error.message ?: "Voice recording failed.", Toast.LENGTH_LONG).show()
            }
        }
    }

    val imagePickerLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.GetMultipleContents()
    ) { uris ->
        if (uris.isEmpty()) {
            isAttachmentMenuOpen = false
            return@rememberLauncherForActivityResult
        }

        val remainingSlots = (MAX_COMPOSER_ATTACHMENTS - attachments.size).coerceAtLeast(0)
        val acceptedUris = uris.take(remainingSlots)
        val droppedCount = uris.size - acceptedUris.size

        if (acceptedUris.isEmpty()) {
            Toast.makeText(context, "You can attach up to $MAX_COMPOSER_ATTACHMENTS images.", Toast.LENGTH_SHORT).show()
            isAttachmentMenuOpen = false
            return@rememberLauncherForActivityResult
        }

        coroutineScope.launch {
            isImportingAttachments = true
            val imported = acceptedUris.mapNotNull { uri ->
                buildCodexImageAttachment(context, uri)
            }
            attachments = (attachments + imported).take(MAX_COMPOSER_ATTACHMENTS)
            isImportingAttachments = false

            if (droppedCount > 0) {
                Toast.makeText(
                    context,
                    "$droppedCount image${if (droppedCount == 1) "" else "s"} skipped. Limit is $MAX_COMPOSER_ATTACHMENTS.",
                    Toast.LENGTH_SHORT
                ).show()
            }
        }

        isAttachmentMenuOpen = false
    }

    fun resetFileAutocompleteState() {
        fileAutocompleteItems = emptyList()
        fileAutocompleteQuery = ""
        isFileAutocompleteVisible = false
        isFileAutocompleteLoading = false
    }

    fun resetSkillAutocompleteState() {
        skillAutocompleteItems = emptyList()
        skillAutocompleteQuery = ""
        isSkillAutocompleteVisible = false
        isSkillAutocompleteLoading = false
    }

    fun resetSlashCommandState() {
        slashCommandItems = emptyList()
        slashCommandQuery = ""
        slashAutocompleteMode = ComposerSlashAutocompleteMode.COMMANDS
    }

    fun hasComposerContentConflictingWithReview(): Boolean {
        val trimmedInput = removingTrailingToken(input, trailingSlashCommandToken(input))
            ?.trim()
            ?: input.trim()
        return trimmedInput.isNotEmpty()
            || attachments.isNotEmpty()
            || mentionedFiles.isNotEmpty()
            || mentionedSkills.isNotEmpty()
            || isPlanModeArmed
    }

    fun selectFileAutocomplete(item: CodexFuzzyFileMatch) {
        val fullPath = item.path.takeIf { it.isNotBlank() } ?: item.resolvedFileName
        replacingTrailingToken(input, trailingFileAutocompleteToken(input), "@$fullPath")?.let {
            input = "$it "
        }

        if (!mentionedFiles.any { it.path == fullPath }) {
            mentionedFiles = mentionedFiles + ComposerMentionedFile(
                fileName = item.resolvedFileName,
                path = fullPath
            )
        }
        resetFileAutocompleteState()
    }

    fun selectSkillAutocomplete(skill: CodexSkillMetadata) {
        val normalizedSkillName = skill.name.trim().removePrefix("/")
        if (normalizedSkillName.isEmpty()) {
            resetSkillAutocompleteState()
            return
        }

        replacingTrailingToken(input, trailingSkillAutocompleteToken(input), "\$$normalizedSkillName")?.let {
            input = "$it "
        }

        if (!mentionedSkills.any { it.name.equals(normalizedSkillName, ignoreCase = true) }) {
            mentionedSkills = mentionedSkills + ComposerMentionedSkill(
                name = normalizedSkillName,
                path = skill.path?.trim()?.takeIf { it.isNotEmpty() },
                description = skill.description
            )
        }
        resetSkillAutocompleteState()
    }

    fun selectSlashCommand(command: ComposerSlashCommand) {
        if (command == ComposerSlashCommand.REVIEW) {
            if (hasComposerContentConflictingWithReview()) {
                Toast.makeText(
                    context,
                    "Clear text, files, skills, and images before starting a code review.",
                    Toast.LENGTH_SHORT
                ).show()
                resetSlashCommandState()
                return
            }
            slashAutocompleteMode = ComposerSlashAutocompleteMode.REVIEW_TARGETS
            return
        }

        if (command == ComposerSlashCommand.FORK) {
            slashAutocompleteMode = ComposerSlashAutocompleteMode.FORK_DESTINATIONS
            return
        }

        if (command == ComposerSlashCommand.STATUS) {
            removingTrailingToken(input, trailingSlashCommandToken(input))?.let {
                input = it
            }
            resetSlashCommandState()
            onShowStatus()
            return
        }

        replacingTrailingToken(input, trailingSlashCommandToken(input), command.cannedPrompt ?: command.token)?.let {
            input = "$it "
        }
        resetSlashCommandState()
    }

    fun selectReviewTarget(target: ComposerReviewTarget) {
        removingTrailingToken(input, trailingSlashCommandToken(input))?.let {
            input = it
        }
        resetSlashCommandState()
        onReviewAction(target)
    }

    fun selectForkDestination(destination: ComposerForkDestination) {
        removingTrailingToken(input, trailingSlashCommandToken(input))?.let {
            input = it
        }
        resetSlashCommandState()
        onForkAction(destination)
    }

    LaunchedEffect(input, autocompleteRoot, isRunning) {
        val fileToken = trailingFileAutocompleteToken(input)
        if (fileToken != null && !autocompleteRoot.isNullOrBlank()) {
            resetSkillAutocompleteState()
            resetSlashCommandState()

            val query = fileToken.query.trim()
            fileAutocompleteQuery = query
            if (query.isEmpty()) {
                fileAutocompleteItems = emptyList()
                isFileAutocompleteVisible = false
                isFileAutocompleteLoading = false
                return@LaunchedEffect
            }

            isFileAutocompleteVisible = true
            isFileAutocompleteLoading = true
            delay(180)

            try {
                val matches = onFuzzyFileSearch(
                    query,
                    listOf(autocompleteRoot),
                    "composer:${draftKey ?: autocompleteRoot}"
                )
                fileAutocompleteItems = matches.take(6)
                isFileAutocompleteVisible = true
                isFileAutocompleteLoading = false
            } catch (error: CancellationException) {
                throw error
            } catch (_: Exception) {
                fileAutocompleteItems = emptyList()
                isFileAutocompleteVisible = false
                isFileAutocompleteLoading = false
            }
            return@LaunchedEffect
        }

        val skillToken = trailingSkillAutocompleteToken(input)
        if (skillToken != null) {
            resetFileAutocompleteState()
            resetSlashCommandState()

            val query = skillToken.query.trim()
            skillAutocompleteQuery = query
            if (query.isEmpty()) {
                skillAutocompleteItems = emptyList()
                isSkillAutocompleteVisible = false
                isSkillAutocompleteLoading = false
                return@LaunchedEffect
            }

            isSkillAutocompleteVisible = true
            isSkillAutocompleteLoading = true
            delay(180)

            try {
                val skills = onListSkills(autocompleteRoot?.let(::listOf), false)
                skillAutocompleteItems = filterSkillAutocompleteItems(skills, query)
                isSkillAutocompleteVisible = true
                isSkillAutocompleteLoading = false
            } catch (error: CancellationException) {
                throw error
            } catch (_: Exception) {
                skillAutocompleteItems = emptyList()
                isSkillAutocompleteVisible = false
                isSkillAutocompleteLoading = false
            }
            return@LaunchedEffect
        }

        val slashToken = trailingSlashCommandToken(input)
        if (slashToken != null) {
            resetFileAutocompleteState()
            resetSkillAutocompleteState()
            slashCommandQuery = slashToken.query
            if (slashAutocompleteMode == ComposerSlashAutocompleteMode.COMMANDS) {
                slashCommandItems = ComposerSlashCommand.filtered(
                    query = slashToken.query,
                    commands = availableSlashCommands
                )
            }
            return@LaunchedEffect
        }

        resetFileAutocompleteState()
        resetSkillAutocompleteState()
        resetSlashCommandState()
    }

    LaunchedEffect(draftKey) {
        cancelVoiceRecording()
    }

    LaunchedEffect(isConnected) {
        if (!isConnected) {
            cancelVoiceRecording()
        }
    }

    DisposableEffect(Unit) {
        onDispose {
            coroutineScope.launch {
                cancelVoiceRecording()
            }
        }
    }

    DisposableEffect(lifecycleOwner, isVoiceRecording, isVoicePreflighting) {
        val observer = LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_STOP && (isVoiceRecording || isVoicePreflighting)) {
                coroutineScope.launch {
                    cancelVoiceRecording()
                }
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose {
            lifecycleOwner.lifecycle.removeObserver(observer)
        }
    }

    fun submitMessage() {
        if (!canSend) return
        if (isRunning) {
            enqueueCurrentDraft()
            return
        }
        onSend(
            input.trim(),
            attachments,
            currentDraftSnapshot().skillMentions,
            if (isPlanModeArmed) CodexCollaborationModeKind.PLAN else null
        )
        clearComposerInputs()
    }

    Column(
        modifier = modifier
            .fillMaxWidth()
            .imePadding()
    ) {
        Column(
            modifier = Modifier.padding(start = 12.dp, end = 12.dp, top = 8.dp),
            verticalArrangement = Arrangement.spacedBy(0.dp)
        ) {
            val composerCardShape = RoundedCornerShape(30.dp)
            Surface(
                modifier = Modifier,
                shape = composerCardShape,
                color = MaterialTheme.colorScheme.surface.copy(alpha = 0.985f),
                tonalElevation = 0.dp,
                shadowElevation = 12.dp,
                border = androidx.compose.foundation.BorderStroke(
                    1.dp,
                    MaterialTheme.colorScheme.outlineVariant.copy(alpha = 0.06f)
                )
            ) {
                Column(
                    modifier = Modifier.padding(start = 16.dp, end = 16.dp, top = 12.dp, bottom = 8.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    QueuedDraftsPanel(
                        drafts = queuedDrafts,
                        pauseMessage = queuedDraftPauseMessage,
                        steeringDraftId = steeringDraftId,
                        onResume = if (queuedDraftPauseMessage != null) {
                            { queuedDraftPauseMessage = null }
                        } else {
                            null
                        },
                        onRestore = ::restoreQueuedDraft,
                        onSteer = { draftId ->
                            val draft = queuedDrafts.firstOrNull { it.id == draftId } ?: return@QueuedDraftsPanel
                            coroutineScope.launch {
                                steeringDraftId = draft.id
                                try {
                                    if (isRunning) {
                                        onSteerQueuedDraft(
                                            draft.text,
                                            draft.attachments,
                                            draft.skillMentions,
                                            draft.collaborationMode
                                        )
                                    } else {
                                        onSend(
                                            draft.text,
                                            draft.attachments,
                                            draft.skillMentions,
                                            draft.collaborationMode
                                        )
                                    }
                                    queuedDrafts = queuedDrafts.filterNot { it.id == draft.id }
                                    queuedDraftPauseMessage = null
                                } catch (error: Exception) {
                                    queuedDraftPauseMessage = error.message ?: "Queue paused after a send failure."
                                    Toast.makeText(
                                        context,
                                        queuedDraftPauseMessage,
                                        Toast.LENGTH_LONG
                                    ).show()
                                } finally {
                                    steeringDraftId = null
                                }
                            }
                        },
                        onRemove = { draftId ->
                            queuedDrafts = queuedDrafts.filterNot { it.id == draftId }
                        }
                    )

                    if (attachments.isNotEmpty()) {
                        AttachmentThumbnailStrip(
                            attachments = attachments,
                            tileSize = 72.dp,
                            onOpen = { previewAttachment = it },
                            onRemove = { attachmentId ->
                                attachments = attachments.filterNot { it.id == attachmentId }
                            }
                        )
                    }

                    if (isImportingAttachments) {
                        Row(
                            verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(8.dp)
                        ) {
                            CircularProgressIndicator(
                                modifier = Modifier.size(14.dp),
                                strokeWidth = 2.dp
                            )
                            Text(
                                text = "Preparing attachments...",
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                    }

                    if (isVoiceRecording) {
                        VoiceRecordingCapsule(
                            audioLevels = voiceAudioLevels,
                            durationMs = voiceRecordingDurationMs,
                            onCancel = {
                                coroutineScope.launch {
                                    cancelVoiceRecording()
                                }
                            }
                        )
                    }

                    if (mentionedFiles.isNotEmpty()) {
                        MentionedFilesRow(
                            files = mentionedFiles,
                            onRemove = { mentionId ->
                                val mention = mentionedFiles.firstOrNull { it.id == mentionId } ?: return@MentionedFilesRow
                                input = removeBoundedToken(input, "@${mention.path}")
                                mentionedFiles = mentionedFiles.filterNot { it.id == mentionId }
                            }
                        )
                    }

                    if (mentionedSkills.isNotEmpty()) {
                        MentionedSkillsRow(
                            skills = mentionedSkills,
                            onRemove = { mentionId ->
                                val mention = mentionedSkills.firstOrNull { it.id == mentionId } ?: return@MentionedSkillsRow
                                input = removeBoundedToken(input, "\$${mention.name}")
                                mentionedSkills = mentionedSkills.filterNot { it.id == mentionId }
                            }
                        )
                    }

                    Box(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(top = 3.dp, bottom = 5.dp)
                    ) {
                        if (input.isBlank()) {
                            Text(
                                text = if (isPlanModeArmed) {
                                    "Start a plan..."
                                } else {
                                    "Ask anything... @files, \$skills, /commands"
                                },
                                style = MaterialTheme.typography.bodySmall,
                                color = ComposerPlaceholderGray
                            )
                        }

                        BasicTextField(
                            value = input,
                            onValueChange = {
                                input = it
                                mentionedFiles = syncMentionedFiles(it, mentionedFiles)
                                mentionedSkills = syncMentionedSkills(it, mentionedSkills)
                            },
                            modifier = Modifier
                                .fillMaxWidth()
                                .defaultMinSize(minHeight = 28.dp)
                                .onFocusChanged { onInputFocusChanged(it.isFocused) },
                            enabled = !isVoiceBusy,
                            minLines = 1,
                            maxLines = 6,
                            keyboardOptions = KeyboardOptions(imeAction = ImeAction.Send),
                            keyboardActions = KeyboardActions(onSend = { submitMessage() }),
                            textStyle = MaterialTheme.typography.bodySmall.merge(
                                TextStyle(color = MaterialTheme.colorScheme.onSurface)
                            ),
                            cursorBrush = SolidColor(MaterialTheme.colorScheme.primary)
                        )
                    }

                    when {
                        isFileAutocompleteVisible -> {
                            FileAutocompletePanel(
                                items = fileAutocompleteItems,
                                isLoading = isFileAutocompleteLoading,
                                query = fileAutocompleteQuery,
                                onSelect = ::selectFileAutocomplete
                            )
                        }
                        isSkillAutocompleteVisible -> {
                            SkillAutocompletePanel(
                                items = skillAutocompleteItems,
                                isLoading = isSkillAutocompleteLoading,
                                query = skillAutocompleteQuery,
                                onSelect = ::selectSkillAutocomplete
                            )
                        }
                        slashCommandItems.isNotEmpty() || slashCommandQuery.isNotEmpty() -> {
                            SlashCommandAutocompletePanel(
                                mode = slashAutocompleteMode,
                                commands = slashCommandItems,
                                query = slashCommandQuery,
                                onSelectCommand = ::selectSlashCommand,
                                onSelectReviewTarget = ::selectReviewTarget,
                                onSelectForkDestination = ::selectForkDestination,
                                onDismissSubmenu = {
                                    slashAutocompleteMode = ComposerSlashAutocompleteMode.COMMANDS
                                }
                            )
                        }
                    }

                    CompositionLocalProvider(LocalMinimumInteractiveComponentEnforcement provides false) {
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            verticalAlignment = Alignment.CenterVertically
                        ) {
                            Box {
                                ComposerGlyphButton(
                                    onClick = { isAttachmentMenuOpen = true },
                                    enabled = !isImportingAttachments && !isRunning && !isVoiceBusy
                                ) {
                                    Icon(
                                        imageVector = Icons.Default.Add,
                                        contentDescription = "Attachment options",
                                        modifier = Modifier.size(18.dp),
                                        tint = ComposerSecondaryGray
                                    )
                                }

                                DropdownMenu(
                                    expanded = isAttachmentMenuOpen,
                                    onDismissRequest = { isAttachmentMenuOpen = false }
                                ) {
                                    DropdownMenuItem(
                                        text = {
                                            Text(
                                                if (isPlanModeArmed) {
                                                    "Plan mode • on"
                                                } else {
                                                    "Plan mode"
                                                }
                                            )
                                        },
                                        leadingIcon = {
                                            Icon(
                                                imageVector = if (isPlanModeArmed) {
                                                    Icons.Default.Check
                                                } else {
                                                    Icons.Default.Psychology
                                                },
                                                contentDescription = null
                                            )
                                        },
                                        onClick = {
                                            isPlanModeArmed = !isPlanModeArmed
                                            isAttachmentMenuOpen = false
                                        },
                                        enabled = !isRunning && !isVoiceBusy
                                    )
                                    DropdownMenuItem(
                                        text = { Text("Camera") },
                                        onClick = {
                                            val hasCameraPermission = ContextCompat.checkSelfPermission(
                                                context,
                                                Manifest.permission.CAMERA
                                            ) == PackageManager.PERMISSION_GRANTED
                                            if (hasCameraPermission) {
                                                launchCameraCapture(takePictureLauncher::launch)
                                            } else {
                                                requestCameraPermissionLauncher.launch(Manifest.permission.CAMERA)
                                            }
                                        },
                                        enabled = attachments.size < MAX_COMPOSER_ATTACHMENTS && !isVoiceBusy
                                    )
                                    DropdownMenuItem(
                                        text = { Text("Photo library") },
                                        onClick = {
                                            imagePickerLauncher.launch("image/*")
                                        },
                                        enabled = attachments.size < MAX_COMPOSER_ATTACHMENTS && !isVoiceBusy
                                    )
                                }
                            }

                            Box {
                                ComposerMetaChip(
                                    label = selectedModelTitle,
                                    leadingIcon = if (selectedServiceTier != null) Icons.Default.Bolt else null,
                                    onClick = { isModelMenuOpen = true },
                                    modifier = Modifier.widthIn(max = 124.dp)
                                )
                                DropdownMenu(
                                    expanded = isModelMenuOpen,
                                    onDismissRequest = { isModelMenuOpen = false }
                                ) {
                                    DropdownMenuItem(
                                        text = { Text(if (selectedModelId == null) "Auto • current" else "Auto") },
                                        onClick = {
                                            onSelectModel(null)
                                            isModelMenuOpen = false
                                        }
                                    )
                                    availableModels.forEach { model ->
                                        DropdownMenuItem(
                                            text = {
                                                Text(
                                                    if (model.id == selectedModelId) {
                                                        "${model.label} • current"
                                                    } else {
                                                        model.label
                                                    }
                                                )
                                            },
                                            onClick = {
                                                onSelectModel(model.id)
                                                isModelMenuOpen = false
                                            }
                                        )
                                    }
                                }
                            }

                            Spacer(modifier = Modifier.width(8.dp))

                            Box {
                                ComposerMetaChip(
                                    label = selectedReasoningLabel,
                                    leadingIcon = Icons.Default.Psychology,
                                    onClick = { isRuntimeMenuOpen = true },
                                    modifier = Modifier.widthIn(max = 116.dp)
                                )
                                DropdownMenu(
                                    expanded = isRuntimeMenuOpen,
                                    onDismissRequest = { isRuntimeMenuOpen = false }
                                ) {
                                    supportedReasoningEfforts.forEach { effort ->
                                        DropdownMenuItem(
                                            text = {
                                                Text(
                                                    if (effort == selectedReasoningEffort) {
                                                        "${effort.replaceFirstChar { it.uppercase() }} • current"
                                                    } else {
                                                        effort.replaceFirstChar { it.uppercase() }
                                                    }
                                                )
                                            },
                                            onClick = {
                                                onSelectReasoningEffort(effort)
                                                isRuntimeMenuOpen = false
                                            }
                                        )
                                    }
                                    DropdownMenuItem(
                                        text = {
                                            Text(
                                                if (selectedReasoningEffort == null) "Auto • current" else "Auto"
                                            )
                                        },
                                        onClick = {
                                            onSelectReasoningEffort(null)
                                            isRuntimeMenuOpen = false
                                        }
                                    )
                                    DropdownMenuItem(
                                        text = {
                                            Text(
                                                if (selectedServiceTier == null) "Normal • current" else "Normal"
                                            )
                                        },
                                        onClick = {
                                            onSelectServiceTier(null)
                                            isRuntimeMenuOpen = false
                                        }
                                    )
                                    CodexServiceTier.entries.forEach { serviceTier ->
                                        DropdownMenuItem(
                                            text = {
                                                Text(
                                                    if (serviceTier == selectedServiceTier) {
                                                        "${serviceTier.displayLabel} • current"
                                                    } else {
                                                        serviceTier.displayLabel
                                                    }
                                                )
                                            },
                                        onClick = {
                                            onSelectServiceTier(serviceTier)
                                            isRuntimeMenuOpen = false
                                        }
                                        )
                                    }
                                }
                            }

                            if (isPlanModeArmed) {
                                Spacer(modifier = Modifier.width(10.dp))
                                Box(
                                    modifier = Modifier
                                        .width(1.dp)
                                        .height(16.dp)
                                        .background(MaterialTheme.colorScheme.outlineVariant.copy(alpha = 0.32f))
                                )
                                Spacer(modifier = Modifier.width(10.dp))
                                Text(
                                    text = "Plan",
                                    style = MaterialTheme.typography.labelMedium,
                                    fontWeight = FontWeight.Medium,
                                    color = AccentPlan
                                )
                            }

                            Spacer(modifier = Modifier.weight(1f))

                            if (isRunning) {
                                ComposerCircleActionButton(
                                    onClick = onStop,
                                    icon = Icons.Default.Stop,
                                    contentDescription = "Stop"
                                )

                                Spacer(modifier = Modifier.width(8.dp))

                                Box {
                                    ComposerCircleActionButton(
                                        onClick = { submitMessage() },
                                        icon = Icons.Default.ArrowUpward,
                                        contentDescription = "Queue draft",
                                        enabled = canSend
                                    )
                                    if (queuedDrafts.isNotEmpty()) {
                                        ComposerQueueBadge(
                                            count = queuedDrafts.size,
                                            modifier = Modifier.align(Alignment.TopEnd)
                                        )
                                    }
                                }
                            } else {
                                ComposerVoiceActionButton(
                                    isBusy = isVoicePreflighting || isVoiceTranscribing,
                                    isRecording = isVoiceRecording,
                                    enabled = !isImportingAttachments && !isVoiceTranscribing,
                                    onClick = {
                                        if (isVoiceRecording) {
                                            coroutineScope.launch {
                                                try {
                                                    stopVoiceRecordingAndTranscribe()
                                                } catch (error: Exception) {
                                                    Toast.makeText(
                                                        context,
                                                        error.message ?: "Voice transcription failed.",
                                                        Toast.LENGTH_LONG
                                                    ).show()
                                                }
                                            }
                                            return@ComposerVoiceActionButton
                                        }

                                        val hasMicrophonePermission = ContextCompat.checkSelfPermission(
                                            context,
                                            Manifest.permission.RECORD_AUDIO
                                        ) == PackageManager.PERMISSION_GRANTED
                                        if (hasMicrophonePermission) {
                                            coroutineScope.launch {
                                                try {
                                                    startVoiceRecording()
                                                } catch (error: Exception) {
                                                    Toast.makeText(
                                                        context,
                                                        error.message ?: "Voice recording failed.",
                                                        Toast.LENGTH_LONG
                                                    ).show()
                                                }
                                            }
                                        } else {
                                            requestMicrophonePermissionLauncher.launch(Manifest.permission.RECORD_AUDIO)
                                        }
                                    }
                                )

                                Spacer(modifier = Modifier.width(8.dp))

                                Box {
                                    ComposerCircleActionButton(
                                        onClick = { submitMessage() },
                                        icon = Icons.Default.ArrowUpward,
                                        contentDescription = "Send",
                                        enabled = canSend
                                    )
                                    if (queuedDrafts.isNotEmpty()) {
                                        ComposerQueueBadge(
                                            count = queuedDrafts.size,
                                            modifier = Modifier.align(Alignment.TopEnd)
                                        )
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    previewAttachment?.let { attachment ->
        AttachmentPreviewDialog(
            attachment = attachment,
            onDismiss = { previewAttachment = null }
        )
    }
}

private fun appendVoiceTranscript(currentInput: String, transcript: String): String {
    val normalizedTranscript = transcript.trim()
    if (normalizedTranscript.isEmpty()) {
        return currentInput
    }

    if (currentInput.isEmpty()) {
        return normalizedTranscript
    }

    return if (currentInput.last().isWhitespace()) {
        currentInput + normalizedTranscript
    } else {
        "$currentInput $normalizedTranscript"
    }
}

@Composable
private fun ComposerMetaChip(
    label: String,
    leadingIcon: androidx.compose.ui.graphics.vector.ImageVector? = null,
    onClick: () -> Unit,
    modifier: Modifier = Modifier
) {
    Surface(
        modifier = modifier.clickable(onClick = onClick),
        color = Color.Transparent,
        tonalElevation = 0.dp
    ) {
        Row(
            modifier = Modifier.padding(horizontal = 4.dp, vertical = 2.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            leadingIcon?.let {
                Icon(
                    imageVector = it,
                    contentDescription = null,
                    modifier = Modifier.size(11.dp),
                    tint = ComposerSecondaryGray
                )
                Spacer(modifier = Modifier.width(6.dp))
            }
            Text(
                text = label,
                style = MaterialTheme.typography.labelSmall,
                fontWeight = FontWeight.Medium,
                maxLines = 1,
                color = ComposerSecondaryGray
            )
            Spacer(modifier = Modifier.width(4.dp))
            Icon(
                imageVector = Icons.Default.KeyboardArrowDown,
                contentDescription = null,
                modifier = Modifier.size(12.dp),
                tint = ComposerSecondaryGray
            )
        }
    }
}

@Composable
private fun ComposerVoiceActionButton(
    isBusy: Boolean,
    isRecording: Boolean,
    enabled: Boolean,
    onClick: () -> Unit,
    modifier: Modifier = Modifier
) {
    if (isBusy || isRecording) {
        ComposerCircleActionButton(
            onClick = onClick,
            icon = if (isRecording) Icons.Default.Stop else Icons.Default.Mic,
            contentDescription = if (isRecording) {
                "Stop voice recording"
            } else {
                "Start voice transcription"
            },
            enabled = enabled,
            modifier = modifier,
            showProgress = isBusy,
            backgroundColor = if (isBusy) {
                MaterialTheme.colorScheme.surfaceVariant
            } else {
                MaterialTheme.colorScheme.onSurface
            },
            contentColor = if (isBusy) {
                MaterialTheme.colorScheme.onSurfaceVariant
            } else {
                MaterialTheme.colorScheme.surface
            }
        )
    } else {
        ComposerGlyphButton(
            onClick = onClick,
            enabled = enabled,
            modifier = modifier
        ) {
            Icon(
                imageVector = Icons.Default.Mic,
                contentDescription = "Start voice transcription",
                modifier = Modifier.size(16.dp),
                tint = ComposerSecondaryGray
            )
        }
    }
}

@Composable
private fun ComposerGlyphButton(
    onClick: () -> Unit,
    enabled: Boolean,
    modifier: Modifier = Modifier,
    content: @Composable () -> Unit
) {
    Surface(
        modifier = modifier
            .size(24.dp)
            .clickable(enabled = enabled, onClick = onClick),
        shape = CircleShape,
        color = Color.Transparent,
        contentColor = ComposerSecondaryGray
    ) {
        Box(contentAlignment = Alignment.Center) {
            content()
        }
    }
}

@Composable
private fun ComposerCircleActionButton(
    onClick: () -> Unit,
    icon: androidx.compose.ui.graphics.vector.ImageVector,
    contentDescription: String,
    enabled: Boolean = true,
    modifier: Modifier = Modifier,
    showProgress: Boolean = false,
    backgroundColor: Color? = null,
    contentColor: Color? = null
) {
    val resolvedBackgroundColor = backgroundColor ?: if (enabled) {
        MaterialTheme.colorScheme.onSurface
    } else {
        MaterialTheme.colorScheme.surfaceVariant
    }
    val resolvedContentColor = contentColor ?: if (enabled) {
        MaterialTheme.colorScheme.surface
    } else {
        MaterialTheme.colorScheme.onSurfaceVariant
    }

    Surface(
        modifier = modifier
            .size(30.dp)
            .clickable(enabled = enabled, onClick = onClick),
        shape = CircleShape,
        color = resolvedBackgroundColor,
        shadowElevation = if (enabled) 8.dp else 0.dp
    ) {
        Box(contentAlignment = Alignment.Center) {
            if (showProgress) {
                CircularProgressIndicator(
                    modifier = Modifier.size(16.dp),
                    strokeWidth = 2.dp,
                    color = resolvedContentColor
                )
            } else {
                Icon(
                    imageVector = icon,
                    contentDescription = contentDescription,
                    modifier = Modifier.size(16.dp),
                    tint = resolvedContentColor
                )
            }
        }
    }
}

@Composable
private fun ComposerQueueBadge(
    count: Int,
    modifier: Modifier = Modifier
) {
    Surface(
        modifier = modifier,
        shape = CircleShape,
        color = AccentPlan
    ) {
        Text(
            text = count.coerceAtMost(9).toString(),
            modifier = Modifier.padding(horizontal = 5.dp, vertical = 1.dp),
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onPrimary,
            fontWeight = FontWeight.SemiBold
        )
    }
}
