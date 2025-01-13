// main.go
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Logger struct and methods
type Logger struct {
    prefix string
}

func NewLogger(prefix string) *Logger {
    return &Logger{prefix: prefix}
}

func (l *Logger) Info(format string, v ...interface{}) {
    log.Printf("[INFO] [%s] %s", l.prefix, fmt.Sprintf(format, v...))
}

func (l *Logger) Error(format string, v ...interface{}) {
    log.Printf("[ERROR] [%s] %s", l.prefix, fmt.Sprintf(format, v...))
}

func (l *Logger) Debug(format string, v ...interface{}) {
    log.Printf("[DEBUG] [%s] %s", l.prefix, fmt.Sprintf(format, v...))
}

// Prometheus metrics
var (
    messageTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "discord_messages_total",
            Help: "Total number of messages per channel",
        },
        []string{"guild_name", "guild_id", "channel_name", "channel_id"},
    )

    messagesByUser = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "discord_messages_by_user_total",
            Help: "Total number of messages per user",
        },
        []string{"guild_name", "guild_id", "user_name", "user_id"},
    )

    voiceTimeSeconds = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "discord_voice_time_seconds",
            Help: "Time spent in voice channels in seconds",
        },
        []string{"guild_name", "guild_id", "user_name", "user_id", "channel_name", "channel_id"},
    )

    voiceChannelMembers = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "discord_voice_channel_members",
            Help: "Number of members in voice channels",
        },
        []string{"guild_name", "guild_id", "channel_name", "channel_id"},
    )

    membersTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "discord_members_total",
            Help: "Total number of members per guild",
        },
        []string{"guild_name", "guild_id"},
    )

    memberJoinsTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "discord_member_joins_total",
            Help: "Total number of member joins",
        },
        []string{"guild_name", "guild_id"},
    )

    memberLeavesTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "discord_member_leaves_total",
            Help: "Total number of member leaves",
        },
        []string{"guild_name", "guild_id"},
    )

    rolesTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "discord_roles_total",
            Help: "Total number of roles",
        },
        []string{"guild_name", "guild_id"},
    )

    membersPerRole = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "discord_members_per_role",
            Help: "Number of members per role",
        },
        []string{"guild_name", "guild_id", "role_name", "role_id"},
    )

    channelsTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "discord_channels_total",
            Help: "Total number of channels by type",
        },
        []string{"guild_name", "guild_id", "type"},
    )
)

func init() {
    // Register all metrics
    prometheus.MustRegister(messageTotal)
    prometheus.MustRegister(messagesByUser)
    prometheus.MustRegister(voiceTimeSeconds)
    prometheus.MustRegister(voiceChannelMembers)
    prometheus.MustRegister(membersTotal)
    prometheus.MustRegister(memberJoinsTotal)
    prometheus.MustRegister(memberLeavesTotal)
    prometheus.MustRegister(rolesTotal)
    prometheus.MustRegister(membersPerRole)
    prometheus.MustRegister(channelsTotal)
}

// Event handler types and methods
type VoiceState struct {
    JoinTime  time.Time
    ChannelID string
}

type EventHandler struct {
    logger      *Logger
    voiceStates struct {
        sync.RWMutex
        states map[string]VoiceState
    }
}

func NewEventHandler() *EventHandler {
    h := &EventHandler{
        logger: NewLogger("HANDLER"),
    }
    h.voiceStates.states = make(map[string]VoiceState)
    return h
}

func (h *EventHandler) Ready(s *discordgo.Session, r *discordgo.Ready) {
    h.logger.Info("Bot is ready! Connected to %d guilds", len(s.State.Guilds))
    
    for _, guild := range s.State.Guilds {
        h.updateGuildMetrics(s, guild)
    }
}

func (h *EventHandler) MessageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
    if m.Author.ID == s.State.User.ID {
        return
    }

    guild, err := s.Guild(m.GuildID)
    if err != nil {
        h.logger.Error("Failed to fetch guild %s: %v", m.GuildID, err)
        return
    }

    channel, err := s.Channel(m.ChannelID)
    if err != nil {
        h.logger.Error("Failed to fetch channel %s: %v", m.ChannelID, err)
        return
    }

    h.logger.Debug("Message from %s in %s/%s", m.Author.Username, guild.Name, channel.Name)

    messageTotal.WithLabelValues(
        guild.Name, guild.ID,
        channel.Name, channel.ID,
    ).Inc()

    messagesByUser.WithLabelValues(
        guild.Name, guild.ID,
        m.Author.Username, m.Author.ID,
    ).Inc()
}

func (h *EventHandler) VoiceStateUpdate(s *discordgo.Session, v *discordgo.VoiceStateUpdate) {
    guild, err := s.Guild(v.GuildID)
    if err != nil {
        h.logger.Error("Failed to fetch guild %s: %v", v.GuildID, err)
        return
    }

    h.voiceStates.Lock()
    defer h.voiceStates.Unlock()

    if v.ChannelID != "" {
        channel, err := s.Channel(v.ChannelID)
        if err != nil {
            h.logger.Error("Failed to fetch channel %s: %v", v.ChannelID, err)
            return
        }

        user, err := s.User(v.UserID)
        if err != nil {
            h.logger.Error("Failed to fetch user %s: %v", v.UserID, err)
            return
        }

        h.logger.Debug("User %s joined voice channel %s in %s", 
            user.Username, channel.Name, guild.Name)

        h.voiceStates.states[v.UserID] = VoiceState{
            JoinTime:  time.Now(),
            ChannelID: v.ChannelID,
        }

        count := 0
        for _, state := range h.voiceStates.states {
            if state.ChannelID == v.ChannelID {
                count++
            }
        }
        voiceChannelMembers.WithLabelValues(
            guild.Name, guild.ID,
            channel.Name, channel.ID,
        ).Set(float64(count))

    } else {
        if state, exists := h.voiceStates.states[v.UserID]; exists {
            duration := time.Since(state.JoinTime).Seconds()
            
            user, _ := s.User(v.UserID)
            channel, _ := s.Channel(state.ChannelID)
            
            if user != nil && channel != nil {
                h.logger.Debug("User %s left voice channel %s in %s after %.2f seconds",
                    user.Username, channel.Name, guild.Name, duration)
                
                voiceTimeSeconds.WithLabelValues(
                    guild.Name, guild.ID,
                    user.Username, v.UserID,
                    channel.Name, channel.ID,
                ).Add(duration)
            }

            delete(h.voiceStates.states, v.UserID)
        }
    }
}

func (h *EventHandler) GuildMemberAdd(s *discordgo.Session, m *discordgo.GuildMemberAdd) {
    guild, err := s.Guild(m.GuildID)
    if err != nil {
        h.logger.Error("Failed to fetch guild %s: %v", m.GuildID, err)
        return
    }

    h.logger.Info("New member %s joined %s", m.User.Username, guild.Name)
    
    memberJoinsTotal.WithLabelValues(guild.Name, guild.ID).Inc()
    membersTotal.WithLabelValues(guild.Name, guild.ID).Inc()
    
    h.updateRoleMetrics(s, guild)
}

func (h *EventHandler) GuildMemberRemove(s *discordgo.Session, m *discordgo.GuildMemberRemove) {
    guild, err := s.Guild(m.GuildID)
    if err != nil {
        h.logger.Error("Failed to fetch guild %s: %v", m.GuildID, err)
        return
    }

    h.logger.Info("Member %s left %s", m.User.Username, guild.Name)
    
    memberLeavesTotal.WithLabelValues(guild.Name, guild.ID).Inc()
    membersTotal.WithLabelValues(guild.Name, guild.ID).Dec()
    
    h.updateRoleMetrics(s, guild)
}

func (h *EventHandler) ChannelCreate(s *discordgo.Session, c *discordgo.ChannelCreate) {
    guild, err := s.Guild(c.GuildID)
    if err != nil {
        h.logger.Error("Failed to fetch guild %s: %v", c.GuildID, err)
        return
    }

    h.logger.Info("New channel %s created in %s", c.Name, guild.Name)
    h.updateChannelMetrics(s, guild)
}

func (h *EventHandler) ChannelDelete(s *discordgo.Session, c *discordgo.ChannelDelete) {
    guild, err := s.Guild(c.GuildID)
    if err != nil {
        h.logger.Error("Failed to fetch guild %s: %v", c.GuildID, err)
        return
    }

    h.logger.Info("Channel %s deleted in %s", c.Name, guild.Name)
    h.updateChannelMetrics(s, guild)
}

func (h *EventHandler) GuildRoleCreate(s *discordgo.Session, r *discordgo.GuildRoleCreate) {
    guild, err := s.Guild(r.GuildID)
    if err != nil {
        h.logger.Error("Failed to fetch guild %s: %v", r.GuildID, err)
        return
    }

    h.logger.Info("New role %s created in %s", r.Role.Name, guild.Name)
    h.updateRoleMetrics(s, guild)
}

func (h *EventHandler) GuildRoleDelete(s *discordgo.Session, r *discordgo.GuildRoleDelete) {
    guild, err := s.Guild(r.GuildID)
    if err != nil {
        h.logger.Error("Failed to fetch guild %s: %v", r.GuildID, err)
        return
    }

    h.logger.Info("Role deleted in %s", guild.Name)
    h.updateRoleMetrics(s, guild)
}

func (h *EventHandler) GuildRoleUpdate(s *discordgo.Session, r *discordgo.GuildRoleUpdate) {
    guild, err := s.Guild(r.GuildID)
    if err != nil {
        h.logger.Error("Failed to fetch guild %s: %v", r.GuildID, err)
        return
    }

    h.logger.Info("Role %s updated in %s", r.Role.Name, guild.Name)
    h.updateRoleMetrics(s, guild)
}

func (h *EventHandler) updateGuildMetrics(s *discordgo.Session, guild *discordgo.Guild) {
    h.logger.Debug("Updating metrics for guild: %s", guild.Name)
    
    membersTotal.WithLabelValues(guild.Name, guild.ID).Set(float64(guild.MemberCount))
    
    h.updateChannelMetrics(s, guild)
    h.updateRoleMetrics(s, guild)
    h.updateVoiceStates(s, guild)
}

func (h *EventHandler) updateChannelMetrics(s *discordgo.Session, guild *discordgo.Guild) {
    var textCount, voiceCount, categoryCount, threadCount, forumCount int

    channels, err := s.GuildChannels(guild.ID)
    if err != nil {
        h.logger.Error("Failed to fetch channels for guild %s: %v", guild.ID, err)
        return
    }

    for _, channel := range channels {
        switch channel.Type {
        case discordgo.ChannelTypeGuildText:
            textCount++
        case discordgo.ChannelTypeGuildVoice:
            voiceCount++
        case discordgo.ChannelTypeGuildCategory:
            categoryCount++
        case discordgo.ChannelTypeGuildNews:
            textCount++ // Count announcement channels as text channels
        case discordgo.ChannelTypeGuildNewsThread, discordgo.ChannelTypeGuildPublicThread, discordgo.ChannelTypeGuildPrivateThread:
            threadCount++
        case discordgo.ChannelTypeGuildForum:
            forumCount++
        }
    }

    h.logger.Debug("Guild %s channels - Text: %d, Voice: %d, Categories: %d, Threads: %d, Forums: %d",
        guild.Name, textCount, voiceCount, categoryCount, threadCount, forumCount)

    channelsTotal.WithLabelValues(guild.Name, guild.ID, "text").Set(float64(textCount))
    channelsTotal.WithLabelValues(guild.Name, guild.ID, "voice").Set(float64(voiceCount))
    channelsTotal.WithLabelValues(guild.Name, guild.ID, "category").Set(float64(categoryCount))
    channelsTotal.WithLabelValues(guild.Name, guild.ID, "thread").Set(float64(threadCount))
    channelsTotal.WithLabelValues(guild.Name, guild.ID, "forum").Set(float64(forumCount))
}

func (h *EventHandler) updateRoleMetrics(s *discordgo.Session, guild *discordgo.Guild) {
    // Get fresh guild data to ensure we have latest roles
    freshGuild, err := s.Guild(guild.ID)
    if err != nil {
        h.logger.Error("Failed to fetch fresh guild data for %s: %v", guild.ID, err)
        return
    }

    // Update total roles count
    rolesTotal.WithLabelValues(guild.Name, guild.ID).Set(float64(len(freshGuild.Roles)))

    // Get all members to calculate role distribution
    members, err := s.GuildMembers(guild.ID, "", 1000)
    if err != nil {
        h.logger.Error("Failed to fetch members for guild %s: %v", guild.ID, err)
        return
    }

    // Count members per role
    roleCounts := make(map[string]int)
    for _, member := range members {
        for _, roleID := range member.Roles {
            roleCounts[roleID]++
        }
    }

    // Update metrics for each role
    for _, role := range freshGuild.Roles {
        count := roleCounts[role.ID]
        h.logger.Debug("Role %s has %d members in %s", role.Name, count, guild.Name)
        membersPerRole.WithLabelValues(
            guild.Name, guild.ID,
            role.Name, role.ID,
        ).Set(float64(count))
    }
}

func (h *EventHandler) updateVoiceStates(s *discordgo.Session, guild *discordgo.Guild) {
    h.voiceStates.RLock()
    defer h.voiceStates.RUnlock()

    // Update current voice sessions
    for userID, state := range h.voiceStates.states {
        if state.ChannelID == "" {
            continue
        }

        channel, err := s.Channel(state.ChannelID)
        if err != nil {
            h.logger.Error("Failed to fetch channel %s: %v", state.ChannelID, err)
            continue
        }

        // Skip if channel is not in this guild
        if channel.GuildID != guild.ID {
            continue
        }

        user, err := s.User(userID)
        if err != nil {
            h.logger.Error("Failed to fetch user %s: %v", userID, err)
            continue
        }

        duration := time.Since(state.JoinTime).Seconds()
        h.logger.Debug("User %s current session in %s/%s: %.2f seconds",
            user.Username, guild.Name, channel.Name, duration)

        voiceTimeSeconds.WithLabelValues(
            guild.Name, guild.ID,
            user.Username, userID,
            channel.Name, channel.ID,
        ).Set(duration)
    }
}

func (h *EventHandler) StartPeriodicUpdates(s *discordgo.Session) {
    ticker := time.NewTicker(30 * time.Second)
    h.logger.Info("Starting periodic metrics update (30-second interval)")
    
    for range ticker.C {
        h.logger.Debug("Running periodic metrics update...")
        
        for _, guild := range s.State.Guilds {
            h.logger.Debug("Updating metrics for guild: %s", guild.Name)
            h.updateGuildMetrics(s, guild)
        }
        
        h.logger.Debug("Periodic update complete")
    }
}

func main() {
    log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
    logger := NewLogger("MAIN")
    logger.Info("Starting Discord Stats Bot...")

    // Create Discord session
    logger.Info("Creating Discord session...")
    dg, err := discordgo.New("Bot " + "MTMyODMxMTY0NjQwOTQ2MTgyMg.GAPSxO.eoFjaR0haaifRYDlS1OrmGdIfcedP1fx5UCOEY")
    if err != nil {
        logger.Error("Failed to create Discord session: %v", err)
        os.Exit(1)
    }

    // Initialize handlers
    handler := NewEventHandler()
    
    // Register all event handlers
    dg.AddHandler(handler.MessageCreate)
    dg.AddHandler(handler.VoiceStateUpdate)
    dg.AddHandler(handler.GuildMemberAdd)
    dg.AddHandler(handler.GuildMemberRemove)
    dg.AddHandler(handler.Ready)
    dg.AddHandler(handler.ChannelCreate)
    dg.AddHandler(handler.ChannelDelete)
    dg.AddHandler(handler.GuildRoleCreate)
    dg.AddHandler(handler.GuildRoleDelete)
    dg.AddHandler(handler.GuildRoleUpdate)

    // Request all intents
    dg.Identify.Intents = discordgo.IntentsAll

    // Start metrics server
    go func() {
        logger.Info("Starting Prometheus metrics server on :2112")
        http.Handle("/metrics", promhttp.Handler())
        if err := http.ListenAndServe(":2112", nil); err != nil {
            logger.Error("Metrics server failed: %v", err)
            os.Exit(1)
        }
    }()

    // Connect to Discord
    logger.Info("Opening Discord websocket connection...")
    if err := dg.Open(); err != nil {
        logger.Error("Failed to open connection: %v", err)
        os.Exit(1)
    }
    defer dg.Close()

    // Start periodic updates
    go handler.StartPeriodicUpdates(dg)

    // Wait for interrupt
    logger.Info("Bot is running. Press CTRL-C to exit.")
    sc := make(chan os.Signal, 1)
    signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
    <-sc
    logger.Info("Shutting down...")
}