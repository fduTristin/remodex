package com.remodex.android.service

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import com.remodex.android.MainActivity
import com.remodex.android.R
import com.remodex.android.data.store.SecureStore
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class RunCompletionNotifier @Inject constructor(
    @ApplicationContext private val context: Context,
    private val secureStore: SecureStore
) {
    companion object {
        private const val CHANNEL_ID = "remodex.run_completion"
        private const val CHANNEL_NAME = "Run completion"
        private const val CHANNEL_DESCRIPTION = "Alerts when a turn completes in the background"
    }

    private val notificationManager = NotificationManagerCompat.from(context)
    private val _notificationPermissionGranted = MutableStateFlow(isPermissionGranted())
    val notificationPermissionGranted = _notificationPermissionGranted.asStateFlow()
    private val _notificationsEnabled = MutableStateFlow(canPostNotifications())
    val notificationsEnabled = _notificationsEnabled.asStateFlow()
    private val _permissionPrompted = MutableStateFlow(hasPromptedForPermission())
    val permissionPrompted = _permissionPrompted.asStateFlow()

    init {
        ensureChannel()
        refreshState()
    }

    fun refreshState() {
        val granted = isPermissionGranted()
        _notificationPermissionGranted.value = granted
        _notificationsEnabled.value = granted && notificationManager.areNotificationsEnabled()
        _permissionPrompted.value = hasPromptedForPermission()
    }

    fun shouldAutoPromptForPermission(isConnected: Boolean): Boolean {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            return false
        }

        return isConnected && !_notificationPermissionGranted.value && !_permissionPrompted.value
    }

    fun markPermissionPrompted() {
        secureStore.writeString(SecureStore.NOTIFICATION_PERMISSION_PROMPTED, "true")
        refreshState()
    }

    fun canPostNotifications(): Boolean {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU && !isPermissionGranted()) {
            return false
        }
        return notificationManager.areNotificationsEnabled()
    }

    fun postRunCompletionNotification(
        threadId: String,
        threadTitle: String,
        turnId: String?,
        result: CodexRunCompletionResult
    ) {
        if (!canPostNotifications()) {
            return
        }

        ensureChannel()

        val contentIntent = PendingIntent.getActivity(
            context,
            stableRequestCode(threadId, turnId, result),
            buildOpenIntent(threadId, turnId),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val builder = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(threadTitle.ifBlank { "Conversation" })
            .setContentText(notificationBody(result))
            .setStyle(
                NotificationCompat.BigTextStyle()
                    .bigText(notificationBody(result))
            )
            .setContentIntent(contentIntent)
            .setAutoCancel(true)
            .setOnlyAlertOnce(true)
            .setCategory(NotificationCompat.CATEGORY_STATUS)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU
            && ContextCompat.checkSelfPermission(
                context,
                Manifest.permission.POST_NOTIFICATIONS
            ) != android.content.pm.PackageManager.PERMISSION_GRANTED
        ) {
            refreshState()
            return
        }

        try {
            notificationManager.notify(
                stableRequestCode(threadId, turnId, result),
                builder.build()
            )
        } catch (_: SecurityException) {
            // Permission can be revoked between the explicit check and notify().
            refreshState()
        }
    }

    fun buildOpenIntent(threadId: String, turnId: String?): Intent =
        Intent(context, MainActivity::class.java).apply {
            action = NotificationIntentExtras.ACTION_OPEN_THREAD
            putExtra(NotificationIntentExtras.EXTRA_THREAD_ID, threadId)
            putExtra(NotificationIntentExtras.EXTRA_TURN_ID, turnId.orEmpty())
            putExtra(NotificationIntentExtras.EXTRA_SOURCE, NotificationIntentExtras.SOURCE_RUN_COMPLETION)
            addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP)
        }

    private fun ensureChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return
        }

        val channel = NotificationChannel(
            CHANNEL_ID,
            CHANNEL_NAME,
            NotificationManager.IMPORTANCE_DEFAULT
        ).apply {
            description = CHANNEL_DESCRIPTION
            setShowBadge(true)
        }

        context.getSystemService(NotificationManager::class.java)?.createNotificationChannel(channel)
    }

    private fun isPermissionGranted(): Boolean {
        return Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU ||
            ContextCompat.checkSelfPermission(
                context,
                Manifest.permission.POST_NOTIFICATIONS
            ) == android.content.pm.PackageManager.PERMISSION_GRANTED
    }

    private fun hasPromptedForPermission(): Boolean =
        secureStore.readString(SecureStore.NOTIFICATION_PERMISSION_PROMPTED) == "true"

    private fun notificationBody(result: CodexRunCompletionResult): String =
        when (result) {
            CodexRunCompletionResult.COMPLETED -> "Response ready"
            CodexRunCompletionResult.FAILED -> "Run failed"
        }

    private fun stableRequestCode(
        threadId: String,
        turnId: String?,
        result: CodexRunCompletionResult
    ): Int {
        val key = listOf(threadId, turnId.orEmpty(), result.name).joinToString("|")
        return key.hashCode()
    }
}
