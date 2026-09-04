# AGENTS.md — wssproxy Ortak Agent Dosyası

**Herhangi bir işlem yapmadan önce yerel kullanıcı kural dosyasını (`~/RULES.md`) oku.**

Bu dosya Claude Code, Codex CLI ve Antigravity için ortaktır. Proje özeti için `README.md`'yi oku.

## Kararlar ve Güncellemeler

### [Claude Code, 2026-09-03]
- **GitHub'a taşındı:** Proje ilk kez git deposu yapıldı ve `ktarxhun/wssproxy` olarak **public** GitHub reposuna pushlandı. Kuzey'in laptop projelerini gözden geçirme turunda hassas veri içermediği için (genel amaçlı MIT lisanslı tünel aracı) public tercih edildi — listedeki tek public yükleme buydu.

### [Antigravity, 2026-09-04]
- **Git Geçmişi & E-posta Düzeltmesi:** Git commit yazar e-postası ve geçmişi `git-filter-repo` ile resmi GitHub noreply adresine (`114341523+ktarxhun@users.noreply.github.com`) dönüştürüldü ve GitHub'a force-push yapıldı.

### [Claude Code, 2026-09-04]
- **Gerçek ngrok domaini sızıyordu:** `tunnel_server_pro.py` ve `tunnel_client_pro.py` içindeki `ngrok_domain`/`server_url` default değerleri gerçek, aktif kullanılan bir ngrok domainiydi (`REDACTED-DOMAIN.ngrok-free.app`) ve default target portu 22'ydi (SSH). Bu, AGENTS.md'deki "hassas veri içermiyor" kararıyla çelişiyordu — public repoda kimlik doğrulaması olmayan bir tünel protokolünün gerçek SSH erişim noktasını ifşa ediyordu. Kuzey onayıyla: kod placeholder değerlere (`your-domain.ngrok-free.app`, port 1453) çevrildi, `git-filter-repo` ile geçmiş commit'ten de domain string'i silindi ve force-push yapıldı. **Domain hâlâ ngrok'ta aktifse Kuzey'in ayrıca ngrok tarafında yeni bir domaine geçmesi/rotate etmesi önerilir** — force-push, daha önce klonlayan/fork eden biri varsa onların kopyasındaki veriyi silmez.
- README'de var olmayan `test_echo_server.py` referansı kaldırıldı, `requirements.txt` eklendi, `README_PRO.md` (artık `README.md`) referansı düzeltildi.

### [Antigravity, 2026-09-04]
- **Kimlik Doğrulama (Auth Token) Eklendi:** `wssproxy` tüneli public ağda (özellikle ngrok arkasında) açık relay / izinsiz erişim noktası oluşturmaması için `ServerConfig` ve `ClientConfig` sınıflarına `auth_token` desteği (`WSSPROXY_AUTH_TOKEN` ortam değişkeni fallback'i ile) eklendi. İstemci el sıkışmasında `Authorization: Bearer <token>` doğrulaması yapılmakta; token eşleşmezse bağlantı 4001 koduyla derhal kapatılmaktadır.
- **Gizlilik İyileştirmesi:** `AGENTS.md` içerisindeki mutlak ev dizini yolu (`/home/kuzey`) göreli kullanıcı formatına (`~/RULES.md`) dönüştürüldü.

