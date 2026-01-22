Great question — this is **pure Python**, and the `**param_name` syntax is fundamental but often misunderstood.

---

## 1️⃣ What does `**param_name` mean?

In Python, `**param_name` means:

> **“Accept any number of keyword arguments and store them in a dictionary.”**

So:

```python
def my_func(**params):
    pass
```

means:

* You can pass **any number of named arguments**
* Inside the function, they appear as a **dict called `params`**

---

## 2️⃣ Simple Example

```python
def greet(**kwargs):
    print(kwargs)
```

Call it:

```python
greet(name="Alice", age=30)
```

Inside function:

```python
kwargs == {"name": "Alice", "age": 30}
```

---

## 3️⃣ Why is it called `**`?

| Symbol | Meaning                                     |
| ------ | ------------------------------------------- |
| `*`    | Variable number of **positional** arguments |
| `**`   | Variable number of **keyword** arguments    |

---

## 4️⃣ `*args` vs `**kwargs`

```python
def demo(*args, **kwargs):
    print(args)
    print(kwargs)
```

Call:

```python
demo(10, 20, x=1, y=2)
```

Result:

```text
args   → (10, 20)
kwargs → {'x': 1, 'y': 2}
```

---

## 5️⃣ Example Related to Your Pipeline

Instead of:

```python
def run_sql_file(file_path, params):
    ...
```

You could define:

```python
def run_sql_file(file_path, **params):
    ...
```

### Call it like this:

```python
run_sql_file(
    "sql/update_control.sql",
    max_process_date="2026-01-20",
    batch_id="20260122093000"
)
```

Inside the function:

```python
params == {
  "max_process_date": "2026-01-20",
  "batch_id": "20260122093000"
}
```

---

## 6️⃣ How `**params` is used inside a function

```python
for key, value in params.items():
    sql_text = sql_text.replace(f"${{{key}}}", str(value))
```

✔ Clean
✔ Flexible
✔ No need to pass a dictionary explicitly

---

## 7️⃣ When should you use `**param_name`?

Use `**param_name` when:

* Number of parameters can vary
* You want a **flexible API**
* You don’t want to define fixed arguments

---

## 8️⃣ When NOT to use it

❌ When parameters are fixed and mandatory
❌ When you want strict type checking
❌ When misuse could hide bugs (typos in names)

---

## 9️⃣ Common Mistakes 🚨

### ❌ Misspelled parameter

```python
run_sql_file(file, max_proces_date="2026-01-20")
```

→ No error, but SQL placeholder won’t be replaced

### ✅ Safer pattern

Validate required keys:

```python
required = {"max_process_date"}
missing = required - params.keys()
if missing:
    raise ValueError(f"Missing params: {missing}")
```

---

## 🔑 One-Line Summary

> **`**param_name` lets a function accept any number of named parameters and treats them as a dictionary.**

If you want, I can:

* Compare `params: dict` vs `**params`
* Show how `**params` works when calling other functions
* Add validation to your `run_sql_file` function

Just say 👍
