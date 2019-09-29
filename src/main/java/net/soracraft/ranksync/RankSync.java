package net.soracraft.ranksync;

import java.sql.*;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import me.lucko.luckperms.api.LuckPermsApi;
import me.lucko.luckperms.api.Node;
import me.lucko.luckperms.api.User;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.Sound;
import org.bukkit.entity.Player;
import org.bukkit.event.Listener;
import org.bukkit.event.EventHandler;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.command.ConsoleCommandSender;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.configuration.file.FileConfiguration;


public final class RankSync extends JavaPlugin implements Listener {

    FileConfiguration config = getConfig();
    private String host, database, username, password;
    private int port;
    private Logger log;
    private boolean use_ssl;
    private Connection connection;

    LuckPermsApi api;

    java.util.Properties conProperties = new java.util.Properties();
    HashMap < Integer, String > groupMap = new HashMap < Integer, String > ();
    HashMap < String, String > rankFormats = new HashMap < String, String > ();

    @Override
    public void onEnable() {

        // load our values from config.yml
        loadConfig();

        // load the group map
        loadGroupMap();

        // set up our logger
        log = getLogger();

        // get our luckperms API instance
        RegisteredServiceProvider < LuckPermsApi > provider = Bukkit.getServicesManager().getRegistration(LuckPermsApi.class);
        if (provider != null) {
            api = provider.getProvider();
        }

        // Enable our class to check for new players using onPlayerJoin()
        getServer().getPluginManager().registerEvents(this, this);

        // open our database connection
        try {
            openConnection();
            Statement statement = connection.createStatement();
        } catch (ClassNotFoundException e) {
            log.warning("Unable to find JDBC, disabling plugin.");
            Bukkit.getPluginManager().disablePlugin(this);
        } catch (SQLException e) {
            log.warning("Error connecting to database, disabling plugin.");
            Bukkit.getPluginManager().disablePlugin(this);
        }

        // start keep alive (query every minute)
        startKeepAlive();
    }

    public void loadConfig() {

        // Creating plugin defaults
        config.options().copyDefaults(true);

        //Save the config
        saveConfig();

        // pull in the connection details from our config.yml file
        host = config.getString("mysql.host");
        port = config.getInt("mysql.port");
        database = config.getString("mysql.database");
        username = config.getString("mysql.username");
        password = config.getString("mysql.password");
        use_ssl = config.getBoolean("mysql.use_ssl");

        // assign our connection properties
        conProperties.put("user", this.username);
        conProperties.put("password", this.password);
        conProperties.put("autoReconnect", "true");
        conProperties.put("maxReconnects", "3");
    }

    public void loadGroupMap() {
        // Add keys and values (Forum group, In game group)
        groupMap.put(3, "admin");
        groupMap.put(4, "mod");
        groupMap.put(6, "builder");
        groupMap.put(7, "supporter");
        groupMap.put(8, "emerald");
        groupMap.put(5, "donator");
        groupMap.put(2, "member");

        // Add keys and values (Rank name, Rank formatting)
        rankFormats.put("supporter", "&d[Supporter]");
        rankFormats.put("emerald", "&a[Emerald]");
        rankFormats.put("donator", "&b[Donator]");
        rankFormats.put("member", "&e[Member]");
    }

    public void openConnection() throws SQLException, ClassNotFoundException {

        // don't reopen the connection if its already open
        if (connection != null && !connection.isClosed()) {
            return;
        }
        synchronized(this) {
            if (connection != null && !connection.isClosed()) {
                return;
            }
            Class.forName("com.mysql.jdbc.Driver");

            // open the database connection using config params
            connection = DriverManager.getConnection("jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database + "?&useSSL=" + this.use_ssl, conProperties);
        }
    }

    public void startKeepAlive() {
        getServer().getScheduler().scheduleSyncRepeatingTask(this, new Runnable() {
            public void run() {

                try {
                    keepAlive(); // exec dummy query on the connection
                } catch (SQLException e) {
                    log.warning("Error when trying to execute the keep-alive");
                }

            }
        }, 0L, 1200L); // re-run in 1 minute
    }

    public void keepAlive() throws SQLException {
        PreparedStatement keepAlive = connection.prepareStatement("SELECT 1 FROM ncms_key");
        keepAlive.executeQuery();
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent event) {

        // get the calling player
        Player player = event.getPlayer();

        // do the group check
        syncRank(player);
    }

    public static String getPlayerGroup(Player player) {

        String[] possibleGroups = {
                "admin",
                "mod",
                "supporter",
                "emerald",
                "donator",
                "member",
                "default"
        };

        for (String group: possibleGroups) {
            if (player.hasPermission("group." + group)) {
                return group;
            }
        }
        return null;
    }

    public void syncRank(Player player) {

        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                ResultSet result = null;
                Statement statement = null;
                UUID uuid = player.getUniqueId();
                String dName = player.getDisplayName();
                User user = api.getUser(uuid);

                try {
                    statement = connection.createStatement();
                    result = statement.executeQuery("SELECT display_style_group_id, bonus_redeemed FROM xf_user WHERE ncms_uuid = '" + uuid.toString() + "';");
                } catch (SQLException e) {
                    log.warning("Error connecting or executing lookup statement");
                    e.printStackTrace();
                }

                try {
                    if (result.next()) {

                        Integer forumGroup = result.getInt("display_style_group_id");
                        String currentGroup = getPlayerGroup(player);
                        String futureGroup = groupMap.get(forumGroup);
                        Integer redeemedStatus = result.getInt("bonus_redeemed");
                        Node newGrp = api.buildNode("group." + futureGroup).setValue(true).build();

                        // check if their group needs to be moved (skip admin and mod)
                        if (!futureGroup.equals("admin") && !futureGroup.equals("mod") && currentGroup != futureGroup) {

                            // clear existing groups and then set the new group
                            user.clearParents();
                            user.setPermission(newGrp);

                            // Now we need to save changes.
                            api.getUserManager().saveUser(user);

                            // send message for donator
                            if (futureGroup == "donator" || futureGroup == "supporter" || futureGroup == "emerald") {

                                sendBroadcast("&b" + dName + "&7 has upgraded to " + rankFormats.get(futureGroup) + "&7 rank, thank you for the support. To view info about ranks check out &d/ranks");
                                playUpgradeSound();

                                // new member, this will only fire if its their first login as a member (redeem status) otherwise it would send when a donator downgrades
                            } else if (futureGroup == "member" && redeemedStatus == 0) {

                                sendBroadcast("&b" + dName + "&7 has registered on our forum and has been moved to the " + rankFormats.get(futureGroup) + "&7 group, and received their rewards. To view benefits for joining (its free!) check out &d/ranks");

                            }

                            // give rewards if not redeemed
                            if (redeemedStatus == 0) {

                                // give key
                                runConsole("eco give " + dName + " 5000"); // 5k eco
                                runConsole("cc give p sora 1 " + dName); // sora key
                                statement.executeUpdate("UPDATE `xf_user` SET `bonus_redeemed` = '1' WHERE `xf_user`.`ncms_uuid` = '" + uuid.toString() + "';"); // set reedeemed
                                sendPlayerMessage(player, "Thanks for registering on our forums, here is &a$5k&7 and a bonus &e[Exotic] Sora Key&7, you can use them at &d/crates&7 and &d/shop");

                            }

                        } else {

                            // no action taken, just log to console
                            log.info("Checked player (" + dName + ") and groups matched (" + futureGroup + ") so no action taken");

                        }



                    } else {
                        log.info("Couldn't find a matching forum user for player (" + dName + ") so no action taken");
                    }
                } catch (SQLException e) {
                    Bukkit.getLogger().warning("Warning with result match");
                    e.printStackTrace();
                }

            }
        };
        r.runTaskAsynchronously(this);
    }

    public static String format(String str) {
        return ChatColor.translateAlternateColorCodes('&', str);
    }

    private void sendBroadcast(String msgString) {
        try {
            boolean success = Bukkit.getScheduler().callSyncMethod(this, new Callable < Boolean > () {
                @Override
                public Boolean call() {
                    ConsoleCommandSender console = Bukkit.getServer().getConsoleSender();
                    String command = "sbc -n &d[&bRanks&d]&7 " + msgString;
                    return Bukkit.dispatchCommand(console, command);
                }
            }).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void runConsole(String consoleCmd) {
        try {
            boolean success = Bukkit.getScheduler().callSyncMethod(this, new Callable < Boolean > () {
                @Override
                public Boolean call() {
                    ConsoleCommandSender console = Bukkit.getServer().getConsoleSender();
                    String command = consoleCmd;
                    return Bukkit.dispatchCommand(console, command);
                }
            }).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void sendPlayerMessage(Player player, String msgString) {
        try {
            boolean success = Bukkit.getScheduler().callSyncMethod(this, new Callable < Boolean > () {
                @Override
                public Boolean call() {
                    player.sendMessage(format("&d[&bRanks&d]&7 " + msgString));
                    return true;
                }
            }).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void playUpgradeSound() {
        for (Player player: Bukkit.getOnlinePlayers()) {
            player.playSound(player.getLocation(), Sound.ENTITY_FIREWORK_ROCKET_TWINKLE, (float) 0.5, 1);
        }
    }


    @Override
    public void onDisable() {}

}