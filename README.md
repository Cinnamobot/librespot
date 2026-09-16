# librespot <sub>+ automix</sub>

**[librespot-org/librespot](https://github.com/librespot-org/librespot) の
フォークです。** ライブラリ本体 — インストール、使い方、オプション、音声
バックエンド、対応プラットフォーム、リリース — は**すべて本家の成果物**です。
**それらについては本家を読んでください。**

> **→ [librespot-org/librespot の README](https://github.com/librespot-org/librespot#readme)**
> **→ [本家の Wiki](https://github.com/librespot-org/librespot/wiki)**(使い方・オプション)
> **→ [COMPILING.md](https://github.com/librespot-org/librespot/blob/master/COMPILING.md)**(ビルド手順)

このフォークが足すのは、**[Fastpotify](https://github.com/Cinnamobot/fastpotify)**
の automix が使う**遷移(トランジション)機構**です。本家には存在しません。
以下はその説明です。

---

## 何を足すのか

本家の再生はギャップレスです。前の曲が終わり次第、次の曲が**1サンプル目から**
鳴ります。2曲が同時に鳴ることはありません。

automix は**2曲を意図的に重ねます**。そのためには、ホスト(クライアント)が
**場所とタイミングを指定**できる必要があります。

```
本家     前の曲 ██████████████│次の曲 ██████████████
                              ↑ ここで切れる。重ならない

automix  前の曲 ██████████████▓▓▓▓▓▓▓▓╗
                              ↑ ここから抜ける
             次の曲 ░░░░░░░░░░░░░░▓▓▓▓▓▓▓▓╝
                                  ↑ ここから鳴る
```

**この「重なり」を成立させるのがこのフォークの役割**です。どこで繋ぐかの判断は
Fastpotify 側が行い、フォークは**指定された通りに鳴らす**ことに責任を持ちます。

---

## 中心となる API

### `CrossfadePlan` — ホストが行き先を指定する

```rust
pub struct CrossfadePlan {
    pub duration: Duration,             // 重なりの長さ
    pub fade_out_before_end: Duration,  // 前の曲の終わりから何秒前で抜け始めるか
    pub fade_in_at: Duration,           // 次の曲のどこから鳴らすか
    pub tempo_rate: f64,                // テンポ比(ピッチは保持)
    pub curve: Option<Arc<IncomingCurve>>,
    pub incoming_track: Option<SpotifyUri>,
}
```

本家の `crossfade` は**長さだけ**でした。`CrossfadePlan` は**場所**を加えます。

| フィールド | 意味 |
|---|---|
| `duration` | 重なりの実時間 |
| `fade_out_before_end` | 前の曲の**終わりからの逆算**。曲末が基準 |
| `fade_in_at` | **次の曲の中の絶対位置**。ここから鳴らす |
| `tempo_rate` | 伸縮比。`curve` がある場合は**終点**の値 |
| `incoming_track` | **この遷移先の曲名** |

### `incoming_track` が必要な理由

**プランは作られた瞬間より長く生き残ります。** 前の曲が鳴っている間ずっと保持され、
差し替えられたり追い越されたりします。だからプランを読む側は、
**それが今の境界のためのプランなのか**を判定できなければなりません。

特に**手動スキップ**でこれが効きます。プランが運ぶ位置は**特定の1曲の中の位置**なので、
別の曲に適用すると**音楽と全く関係ない場所**にシークしてしまいます。名前を照合して
一致した時だけ適用することで、これを防ぎます。

```
プラン: 「spotify:track:AAA の 20.73s から鳴らす」
                    │
        スキップ先と一致する? ── yes ──→ 20.73s から開始
                    │
                    no ──→ 0s から開始(従来どおり)
```

---

## タイムライン — 両デッキがテンポを共有する

重なりの間、**2つのデッキは同じ実効テンポを共有**します。テンポは前の曲のものから
次の曲のものへ**滑らかに滑ります**。

```
実時間 →    0s                    5.22s                 10.44s
            │                     │                     │
共有テンポ  92.00 BPM             95.43 BPM             98.99 BPM
            │                     │                     │
前の曲      262.58s               267.90s               273.41s
  レート    1.000x                1.037x                1.076x
  ゲイン    1.00                  0.71                  0.00   (cos)
            │                     │                     │
次の曲      20.73s                25.67s                30.80s
  レート    0.929x                0.964x                1.000x
  ゲイン    0.00                  0.71                  1.00   (sin)
```

**始点では両方が前の曲のテンポ、終点では両方が次の曲のテンポ**です。途中も常に
一致します — 前の曲を `ratio^p`、次の曲を `ratio^(p-1)` で走らせると、実効テンポは
どちらも `out_bpm × ratio^p` になるからです。

> 📌 **聴いている側の曲は、遷移の最初と最後で自然なテンポのままです。**
> 始点では前の曲が自分のテンポ(1.0x)、終点では次の曲が自分のテンポ(1.0x)。
> 伸縮は2つのデッキの間を**移っていく**だけで、どちらもロックされたままです。
>
> 直線的なランプではこれが成立しません。`1.0` から始まって `1/ratio` で終わる直線と、
> `ratio` から始まって `1.0` で終わる直線は、商が一定になりません。**幾何級数
> (geometric)だけがこの形を作れます。**

### ピッチは保持される

`tempo_rate` の伸縮は **keylock** を通します。テンポが変わっても**ピッチは変わりません**。
`EngineProfile::Keylock` のデッキが回路の終端まで準備されます。

### フェード形状は等パワー

`cos` / `sin` の等パワーです。**線形だと重なりの中央で約3 dB 落ちます** —
別々の曲は相関がないので、線形フェードは音量が下がって聞こえます。

`Ramp` は最初のフレームを始点、最後のフレームを終点として扱うので、
**カーブはちょうど 0 と 1 に到達します**。そうしないと次の曲がフルレベルに
届かず、前の曲がまだ聞こえるうちに切れます。

---

## 入場側は事前レンダリング

次の曲の伸縮は、**境界の手前でレンダリング**して `IncomingCurve` として
再生します。ライブで走らせない理由があります。

```
ライブで走らせる場合:
  デコーダ ── パケット単位で供給 ──→ デッキ ── 掃引レートで消費 ──→ sink
                    ↑
              供給できるループは位置報告ループだけ
              → アンダーラン か デコーダが聴取位置を追い越す

事前レンダリングの場合:
  デコーダ ──→ デッキ(オフライン) ──→ 完成したバッファ ──→ sink
                                        ↑
                                  期限がない。追い越しが起きない
```

オフラインなら**期限がありません**。ソースは空きがあるまで押し込まれ、レンダリングは
重なりが埋まるまで引かれます。**両者が相手を追い越せない**という構造です。

`IncomingCurve` は**消費したフレーム数**も持ちます。デコーダは自分のレートで
パケット単位に要求されるため、重なりの終わりには**聴取位置より先へ進んでいます**。
その差がこれで、**レンダリングの性質として閉じた形**で求まります(再導出しません)。

---

## ホストが必要とするイベント

automix は**次の曲の情報を、音声より早く**必要とします。

### `UpcomingTrack` — 次に鳴る曲を、キューが知った時点で

```
本家          曲末が近づく ──→ 発火
                           ↑ 音声を用意する時間しかない

このフォーク  キューが次曲を知る ──→ 発火
                                  ↑ 調べる時間が十分ある
```

`TimeToPreloadNextTrack` は**音声**を時間内に用意するためのものなので、曲がほぼ
終わるまで発火しません。**次曲の正体だけ**を知りたいホストは、もっと早くから
知ることができます。

### `IncomingPreloaded` — 次曲の短いプローブ

**sink には前の曲しか届きません。** 次に鳴る曲は一度もミキサーを通らないので、
その曲の音声を解析する手段が別に必要です。これがその手段で、
**プローブした音声とその位置**を運びます。

### `read_is_ready` — 音声スレッドを止めない

プローブは **sink を供給しているのと同じスレッド**で走ります。そして
`AudioFileStreaming::read` は**要求したバイトが届くまで Condvar でブロック**します。
到着前にステップを踏むと、**再生中の曲が CDN の応答時間ぶん止まります**。

```
未到着のまま read ──→ Condvar で待つ ──→ sink が枯れる ──→ 音が切れる
                  ↑
                  実測: 中央値 137 ms、最悪数秒

read_is_ready が false  ──→ そのステップを踏まず次の周回へ
```

### プリロード要求の再試行

`TimeToPreloadNextTrack` は**1曲につき1回**しか上がりません。受け手が答えられない
瞬間があると、**その曲のプリロードは永久に失われます**。

実際に起きたのは**単曲を再生した場合**です。キューの先頭に区切りマーカーが入り、
`spotify:delimiter` は**どの曲にも解釈できない**ので、要求は消費されて何も
プリロードされません。要求を出したのに何も届かなければ、**2秒間隔で最大8回**まで
再要求します(上限があるのは、文脈の最後の曲には本当に次が無いためです)。

---

## 帰属

> **基底のクロスフェードは本フォークの成果ではありません。**
> [librespot-org/librespot#1756](https://github.com/librespot-org/librespot/pull/1756)
> "feat(playback): crossfade between tracks"(@revolutionxk、本家でオープン中)
> のチェリーピックです。
>
> **2つ目のデコーダ、等パワーランプ、sink 手前でのミックス、
> `PlayerConfig::crossfade` はその PR の成果です。** 本フォークはそれを
> 自分の変更の上に載せ直しただけです。**機構そのものの議論は本家 PR が
> 適切な場所です。**

このフォークが追加するのは、その上に載る**計画とタイミング**の層です。

---

## 使い方

librespot のクレートは**すべてこのフォークから**取る必要があります
(1つのコピーしか存在できないため)。

```toml
[patch.crates-io]
librespot-audio    = { git = "https://github.com/Cinnamobot/librespot", branch = "main" }
librespot-connect  = { git = "https://github.com/Cinnamobot/librespot", branch = "main" }
librespot-core     = { git = "https://github.com/Cinnamobot/librespot", branch = "main" }
librespot-metadata = { git = "https://github.com/Cinnamobot/librespot", branch = "main" }
librespot-oauth    = { git = "https://github.com/Cinnamobot/librespot", branch = "main" }
librespot-playback = { git = "https://github.com/Cinnamobot/librespot", branch = "main" }
librespot-protocol = { git = "https://github.com/Cinnamobot/librespot", branch = "main" }
```

`Cargo.lock` がリビジョンを固定するので、解決は再現します。

> ⚠️ **本家の品質ワークフローはこのフォークでは走りません。**
> 本家の `quality.yml` / `build.yml` は `dev` と `master` だけを対象にしています。
> 変更を push する前に、そのジョブが走らせるものを手元で実行してください:
>
> ```shell
> cargo fmt --all --check
> cargo clippy --all-targets -- -D warnings
> cargo test
> ```

---

## 変更箇所

| クレート | 内容 |
|---|---|
| `playback/src/player.rs` | `CrossfadePlan`、デッキ、伸縮、ベース受け渡し、プローブ、イベント |
| `audio/src/fetch/mod.rs` | `read_is_ready`(プローブのステップを後回しにする) |
| `connect/src/spirc.rs` | キューが動いた時に `UpcomingTrack` を上げる |
| `connect/src/state/tracks.rs` | 次に**鳴らせる**曲を返す(区切りマーカーを飛ばす) |
| `protocol/build.rs` | `cuepoints.proto`(automix のキュー)をコンパイルする |

変更は**計測値つきの個別コミット**として保たれているので、どれか1つを外したり、
本家へ個別に送ったりできます。本家が取り込んだパッチは、取り込まれた時点で
このフォークから外してください。

---

## ライセンス

本家 [librespot-org/librespot](https://github.com/librespot-org/librespot) と
同じ MIT です。Spotify とは無関係です。Spotify は Spotify AB の商標です。
