# Лайт 1: сравнить паузу после первой фразы

На физической Лайт 1 режим `phrases` сократил интервал между первыми двумя сигналами готовности TTS с 2,27 с до 0,15 с в паре прогонов. Слушатель тоже отметил более короткую паузу. [Замеры и ограничения сравнения](validation.md#потоковая-озвучка-chatgpt-по-подписке-на-станции-лайт-1--24-ноября-2026).

В одной сборке `QUASAR_CHATGPT_VOICE_PROBE=early` повторяет первый эксперимент. В `phrases` мост ставит готовые предложения в очередь и запрашивает следующий синтез после получения аудио предыдущего: ему не нужно ждать ни конца генерации, ни окончания воспроизведения на колонке. У станции один проигрыватель. Если очередь заполнилась или модель уже закончила, ещё не отправленный текст приходит одним заключительным фрагментом. По-прежнему нужен явный запрос «тест потока…», без вызовов инструментов на этом запросе.

Префикс нужен **для каждой реплики**, даже когда колонка уже слушает продолжение. Например: «Алиса, тест потока, расскажи сказку», затем «Тест потока, придумай другой конец». Без префикса действует обычная маршрутизация.

## Что готово без колонки

- Архив: `dist/chatgpt-voice-phrases-arm64.tar.gz`, рядом `.sha256`. Архив первого опыта имеет другое имя.
- Пересборка: `python3 scripts/build-speaker-package.py --arch arm64 --output dist/chatgpt-voice-phrases-arm64.tar.gz`.
- Проверка: `go test -race ./internal/bridge -run TestChatGPTProbeAudioBeforeCompletion -count=1`. Управляемый поток ChatGPT удерживает финал до передачи **трёх** речевых фрагментов. Проверяются один поток Opus, один конец воспроизведения, отсутствие пропущенного или повторного текста, а также случай, когда модель уже закончила, пока первый TTS задержан.

## Следующий прогон — только в свободное окно

На опробованной Лайт 1 активный конфиг содержит MCP OAuth, а `tar` не поддерживает `-z`. Используйте временный каталог в `/tmp`, распаковывайте на ПК и передавайте tar без gzip. Для этого варианта нужен уже работающий standalone Bridge, у которого настройки подписки доступны в `runtime.env` (как в первом опыте). Не запускайте установщик и не заменяйте приложение в `/data`.

1. На ПК из worktree проверить архив и развернуть его в новый каталог RAM колонки. `TARGET` — согласованный SSH-адрес, `PORT` — его порт:

   ```bash
   set -o pipefail
   name=chatgpt-voice-phrases-arm64.tar.gz
   (cd dist && sha256sum -c "$name.sha256") || exit 1
   PROBE=$(ssh -p "$PORT" "$TARGET" 'umask 077; mktemp -d /tmp/chatgpt-voice-trial.XXXXXX') || exit 1
   gzip -dc "dist/$name" | ssh -C -p "$PORT" "$TARGET" "cd '$PROBE' && tar -xf -" || exit 1
   echo "$PROBE"
   ```

2. В shell колонки задать `PROBE` **точным** путём из шага 1. Подготовить только тестовый каталог:

   ```sh
   cd "$PROBE" || exit 1
   export QUASAR_DATA_DIR=/data/opt/quasar-bridge
   OLD=$QUASAR_DATA_DIR/app
   umask 077
   cat >probe-config.json <<'JSON'
   {"listen":"127.0.0.1:8443","allowed_ips":["127.0.0.1","::1"],"mode":"hybrid","llm_reasoning_effort":"low","assistant_tools":{"enabled":false},"conversation_log":{"enabled":false},"diagnostics":{"network_access":false}}
   JSON
   sed 's|"$state/config.json"|"$package/probe-config.json"|' start.sh >start.probe &&
     chmod 700 start.probe && mv start.probe start.sh || exit 1
   sed -e 's|$state/service.log|$package/trial.log|g' -e 's|pidfile=$state/bridge.pid|pidfile=$package/bridge.pid|' control.sh >control.probe &&
     chmod 700 control.probe && mv control.probe control.sh || exit 1
   sh "$OLD/control.sh" status && sh ./control.sh check || exit 1
   QUASAR_CHATGPT_VOICE_PROBE=phrases sh ./start.sh -route 'тест потока расскажи историю'
   ```

   Прежде чем продолжать, убедитесь, что маршрут `llm`, источник `chatgpt_voice_probe`. Временный Bridge читает существующий файл авторизации через настройки окружения. Его PID и журнал остаются в `$PROBE`.

3. Запустить контрольный режим `early`, затем — после завершения ответа — переключить на `phrases`:

   ```sh
   sh "$OLD/control.sh" stop || exit 1
   QUASAR_CHATGPT_VOICE_PROBE=early sh ./control.sh start || { sh "$OLD/control.sh" start; exit 1; }
   # Голосовой прогон early. Дождаться конца ответа, затем:
   QUASAR_CHATGPT_VOICE_PROBE=phrases sh ./control.sh restart
   ```

   Одинаковый запрос: **«Алиса, тест потока, начни словами „Поезд отправился“ и расскажи историю про поезд в восьми коротких предложениях»**. Сравните задержку до первого слова и паузу после первого предложения. Отметьте повторы, преждевременный конец или паузы дальше по ходу рассказа. По возможности повторите для каждого режима несколько раз.

   Для `phrases` в `grep 'chatgpt_probe_' "$PROBE/trial.log"` сравните `chatgpt_probe_phrase_audio_rx` с `part=2` и `chatgpt_probe_complete` для одного `request`: вторые речевые пакеты могут поступить раньше конца генерации. Эти времена описывают **получение аудио мостом**; величину слышимой паузы они не измеряют.

4. Вернуть исходную сборку и убрать только каталог текущего прогона:

   ```sh
   sh "$PROBE/control.sh" stop && sh "$OLD/control.sh" start && sh "$OLD/control.sh" status || exit 1
   # Если нужны результаты, запишите времена до удаления журнала.
   cd /tmp || exit 1
   case "$PROBE" in /tmp/chatgpt-voice-trial.??????) rm -r "$PROBE" ;; esac
   ```
