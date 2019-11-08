# soraRankSync

A Paper plugin that synced players' in-game ranks with their forum accounts on the Soracraft Minecraft server.

> Archived. No longer maintained.

When a player joins, or runs `/checkrank` to check their own rank, the plugin looks up the player's UUID in the XenForo forum's MySQL database (`xf_user`), maps their forum group to an in-game group, and moves them to that group through LuckPerms if it differs. Admin and mod groups are left alone. Upgrades to a donor rank are announced in chat. On a player's first move, the plugin also gives a one-time forum sign-up reward through console commands (`eco give` and `cc give`) and marks it as redeemed in the forum database. The forum-to-game group mapping is hard-coded in `RankSync.java`.

## Stack

- Java 8, Paper API 1.14.4, LuckPerms API 4.3
- MySQL over plain JDBC (`com.mysql.jdbc.Driver`, which the server must provide); HikariCP is declared in `pom.xml` but not used
- Maven, with the shade plugin producing the jar

## Develop

```sh
mvn package   # builds target/ranksync-1.0.jar
```

Database connection settings go in the plugin's `config.yml` (`mysql.host`, `port`, `database`, `username`, `password`, `use_ssl`).

## License

[MIT](LICENSE)
