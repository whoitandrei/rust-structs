# примитивы синхронизации и потокобезопасные структуры на Rust

коллекция структур данных и примитивов синхронизации, написанных вручную. Цель - написать все самому на Rust чтобы разобраться в его устройстве

## Стек

- **Язык:** Rust
- **Зависимости:** [`atomic-wait`](https://crates.io/crates/atomic-wait)
- **CI:** GitHub Actions (`.github/workflows`).

## Сборка и тесты

```
cargo build
cargo test
```

## Структура проекта

```
rust-structs/
├── Cargo.toml
├── Cargo.lock
├── .github/workflows/   CI
└── src/
    ├── lib.rs           <публичные модули>
    ├── <primitives>/    <примитивы синхронизации: SpinLock, Mutex, Semaphore, Barrier, Once, RWLock>
    ├── <blocking>/      <blocking-структуры: BoundedQueue>
    └── <lockfree>/      <lock-free структуры: Ring Buffer>
```
