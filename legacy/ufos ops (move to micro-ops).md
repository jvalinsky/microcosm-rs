ufos ops

btrfs snapshots: snapper

```bash
sudo apt install snapper
sudo snapper -c ufos-db create-config /mnt/ufos-db

# edit /etc/snapper/configs/ufos-db
# change
TIMELINE_MIN_AGE="1800"
TIMELINE_LIMIT_HOURLY="10"
TIMELINE_LIMIT_DAILY="10"
TIMELINE_LIMIT_WEEKLY="0"
TIMELINE_LIMIT_MONTHLY="10"
TIMELINE_LIMIT_YEARLY="10"
# to
TIMELINE_MIN_AGE="1800"
TIMELINE_LIMIT_HOURLY="22"
TIMELINE_LIMIT_DAILY="4"
TIMELINE_LIMIT_WEEKLY="0"
TIMELINE_LIMIT_MONTHLY="0"
TIMELINE_LIMIT_YEARLY="0"
```

this should be enough?

list snapshots:

```bash
sudo snapper -c ufos-db list
```

systemd

create file: `/etc/systemd/system/ufos.service`

```ini
[Unit]
Description=UFOs-API
After=network.target

[Service]
User=pi
WorkingDirectory=/home/pi/
ExecStart=/home/pi/ufos --jetstream us-west-2 --data /mnt/ufos-db/
Environment="RUST_LOG=info"
LimitNOFILE=16384
Restart=always

[Install]
WantedBy=multi-user.target
```

then

```bash
sudo systemctl daemon-reload
sudo systemctl enable ufos
sudo systemctl start ufos
```

monitor with

```bash
journalctl -u ufos -f
```

make sure a backup dir exists

```bash
mkdir /home/pi/backup
```

mount the NAS

```bash
sudo mount.cifs "//truenas.local/folks data" /home/pi/backup -o user=phil,uid=pi
```

manual rsync

```bash
sudo rsync -ahP --delete /mnt/ufos-db/.snapshots/1/snapshot/ backup/ufos/
```

backup script sketch

```bash
NUM=$(sudo snapper --csvout -c ufos-db list --type single --columns number | tail -n1)
sudo rsync -ahP --delete "/mnt/ufos-db/.snapshots/${NUM}/snapshot/" backup/ufos/
```

just crontab it?

`sudo crontab -e`
```bash
0 1/6 * * * rsync -ahP --delete "/mnt/ufos-db/.snapshots/$(sudo snapper --csvout -c ufos-db list --columns number | tail -n1)/snapshot/" backup/ufos/
```

^^ try once initial backup is done


--columns subvolume,number

subvolume
number




gateway: follow constellation for nginx->prom thing

config at `/etc/prometheus-nginxlog-exporter.hcl`

before: `/etc/prometheus-nginxlog-exporter.hcl`

```hcl
listen {
  port = 4044
}

namespace "nginx" {
  source = {
    files = [
      "/var/log/nginx/constellation-access.log"
    ]
  }

  format = "$remote_addr - $remote_user [$time_local] \"$request\" $status $upstream_cache_status $body_bytes_sent \"$http_referer\" \"$http_user_agent\" \"$http_x_forwarded_for\""

  labels {
    app = "constellation"
  }

  relabel "cache_status" {
    from = "upstream_cache_status"
  }
}
```

after:

```hcl
listen {
  port = 4044
}

namespace "constellation" {
  source = {
    files = [
      "/var/log/nginx/constellation-access.log"
    ]
  }

  format = "$remote_addr - $remote_user [$time_local] \"$request\" $status $upstream_cache_status $body_bytes_sent \"$http_referer\" \"$http_user_agent\" \"$http_x_forwarded_for\""

  labels {
    app = "constellation"
  }

  relabel "cache_status" {
    from = "upstream_cache_status"
  }

  namespace_label = "vhost"
  metrics_override = { prefix = "nginx" }
}

namespace "ufos" {
  source = {
    files = [
      "/var/log/nginx/ufos-access.log"
    ]
  }

  format = "$remote_addr - $remote_user [$time_local] \"$request\" $status $upstream_cache_status $body_bytes_sent \"$http_referer\" \"$http_user_agent\" \"$http_x_forwarded_for\""

  labels {
    app = "ufos"
  }

  relabel "cache_status" {
    from = "upstream_cache_status"
  }

  namespace_label = "vhost"
  metrics_override = { prefix = "nginx" }
}
```


```bash
systemctl start prometheus-nginxlog-exporter.service
```

