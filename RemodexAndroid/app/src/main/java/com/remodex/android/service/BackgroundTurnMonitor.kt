package com.remodex.android.service

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import com.remodex.android.MainActivity
import com.remodex.android.R
import dagger.hilt.android.AndroidEntryPoint
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class BackgroundTurnMonitor @Inject constructor(
    @ApplicationContext private val context: Context
) {
    companion object {
        private const val TAG = "BackgroundTurnMonitor"
    }

    private var wasRunning = false
    private var lastRunningThreadCount = 0

    fun sync(appInForeground: Boolean, runningThreadCount: Int, enabled: Boolean) {
        val shouldRun = enabled && !appInForeground && runningThreadCount > 0
        val shouldRefreshNotification = shouldRun &&
            (!wasRunning || lastRunningThreadCount != runningThreadCount)
        Log.d(
            TAG,
            "sync foreground=$appInForeground running=$runningThreadCount enabled=$enabled shouldRun=$shouldRun wasRunning=$wasRunning lastCount=$lastRunningThreadCount"
        )

        if (shouldRefreshNotification) {
            runCatching {
                Log.d(TAG, "Starting background turn monitor service")
                BackgroundTurnMonitorService.start(context, runningThreadCount)
            }.onFailure { error ->
                Log.w(TAG, "Unable to start background turn monitor: ${error.message}")
            }
        } else if (!shouldRun && wasRunning) {
            Log.d(TAG, "Stopping background turn monitor service")
            BackgroundTurnMonitorService.stop(context)
        }
        wasRunning = shouldRun
        lastRunningThreadCount = if (shouldRun) runningThreadCount else 0
    }
}

@AndroidEntryPoint
class BackgroundTurnMonitorService : Service() {
    companion object {
        private const val TAG = "BackgroundTurnMonitorSvc"
        private const val CHANNEL_ID = "remodex.run_monitor"
        private const val CHANNEL_NAME = "Background run monitoring"
        private const val CHANNEL_DESCRIPTION = "Keeps Remodex connected while runs finish in the background"
        private const val NOTIFICATION_ID = 41_002
        private const val EXTRA_RUNNING_THREAD_COUNT = "running_thread_count"
        private const val POLL_INTERVAL_MS = 2_000L

        fun start(context: Context, runningThreadCount: Int) {
            val intent = Intent(context, BackgroundTurnMonitorService::class.java)
                .putExtra(EXTRA_RUNNING_THREAD_COUNT, runningThreadCount)
            ContextCompat.startForegroundService(context, intent)
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, BackgroundTurnMonitorService::class.java))
        }
    }

    @Inject lateinit var codexService: CodexService

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private var pollJob: Job? = null

    override fun onCreate() {
        super.onCreate()
        ensureChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val runningThreadCount = maxOf(intent?.getIntExtra(EXTRA_RUNNING_THREAD_COUNT, 1) ?: 1, 1)
        Log.d(TAG, "onStartCommand runningThreadCount=$runningThreadCount startId=$startId")
        if (ContextCompat.checkSelfPermission(
                this,
                Manifest.permission.FOREGROUND_SERVICE
            ) != PackageManager.PERMISSION_GRANTED
        ) {
            Log.w(TAG, "Foreground service permission is unavailable")
            stopSelf(startId)
            return START_NOT_STICKY
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE
            && ContextCompat.checkSelfPermission(
                this,
                Manifest.permission.FOREGROUND_SERVICE_DATA_SYNC
            ) != PackageManager.PERMISSION_GRANTED
        ) {
            Log.w(TAG, "Foreground service permission is unavailable")
            stopSelf(startId)
            return START_NOT_STICKY
        }
        try {
            startForeground(NOTIFICATION_ID, buildNotification(runningThreadCount))
        } catch (error: SecurityException) {
            Log.w(TAG, "Unable to start foreground turn monitor: ${error.message}")
            stopSelf(startId)
            return START_NOT_STICKY
        }
        if (pollJob?.isActive != true) {
            pollJob = scope.launch {
                while (isActive) {
                    Log.d(TAG, "Polling running turn states")
                    codexService.refreshTrackedRunningTurnStatesNow()
                    delay(POLL_INTERVAL_MS)
                }
            }
        }
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        Log.d(TAG, "onDestroy")
        pollJob?.cancel()
        pollJob = null
        scope.cancel()
        stopForeground(STOP_FOREGROUND_REMOVE)
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun ensureChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return
        }

        val channel = NotificationChannel(
            CHANNEL_ID,
            CHANNEL_NAME,
            NotificationManager.IMPORTANCE_LOW
        ).apply {
            description = CHANNEL_DESCRIPTION
            setShowBadge(false)
        }

        getSystemService(NotificationManager::class.java)?.createNotificationChannel(channel)
    }

    private fun buildNotification(runningThreadCount: Int) =
        NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle("Keeping Remodex connected")
            .setContentText(
                if (runningThreadCount == 1) {
                    "Monitoring 1 background run"
                } else {
                    "Monitoring $runningThreadCount background runs"
                }
            )
            .setContentIntent(buildOpenAppIntent())
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setSilent(true)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()

    private fun buildOpenAppIntent(): PendingIntent {
        val intent = Intent(this, MainActivity::class.java).apply {
            addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP)
        }
        return PendingIntent.getActivity(
            this,
            0,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
    }
}
