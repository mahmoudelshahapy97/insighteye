
---

## 🟢 1. افتح tmux session جديدة

```bash
tmux new -s insighteye_store
```

---

## 🟢 2. شغّل الأمر داخل الـ tmux

بعد ما تدخل على الـ tmux session نفّذ:

```bash
docker exec -it insighteye_app bash -c "python3 scripts/store_data_labeled.py"
```

---

## 🟢 3. افصل الـ session وسيبه شغال في الخلفية

اضغط:

```
Ctrl + B ثم D
```

كده السكربت هيكمل شغال حتى لو قفلت الاتصال.

---

## 🟢 4. للرجوع للـ session تاني

```bash
tmux attach -t insighteye_store
tmux attach -t insighteye_store_with
```

---

## 🟢 5. لو عايز تشغّله مباشرة في الخلفية (one-liner)

من غير ما تدخل على tmux:

```bash
tmux new -d -s insighteye_store "docker exec -it insighteye_app bash -c 'python3 scripts/store_data_labeled.py'"
tmux new -d -s insighteye_store "docker exec -it insighteye_app bash -c 'python3 scripts/store_data_test.py'"
```

---

## 🟢 6. تشوف الـ sessions اللي شغالة

```bash
tmux ls
```

## 🟢 7. تشوف عدد الصور

```bash
find images_test/ -type f | wc -l
find images_labeled/ -type f | wc -l

tar -czvf images_test.tar.gz images_test/
tar -czvf images_labeled.tar.gz images_labeled/

zip -r images_test.zip images_test/
zip -r -9 images_test.zip images_test/
zip -r images_labeled.zip images_labeled/
zip -r -9 images_labeled.zip images_labeled/
```
