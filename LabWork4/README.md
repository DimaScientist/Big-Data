# Лабораторная работа № 4: Zookeeper
***

## Цель работы:

 * запустить ZooKeeper,
 * изучить директорию с установкой ZooKeeper,
 * запустить интерактивную сессию ZooKeeper CLI и освоить её команды,
 * научиться проводить мониторинг ZooKeeper,
 * разработать приложение с барьерной синхронизацией, основанной на ZooKeeper,
 * запустить и проверить работу приложения.


## Выполнение заданий:

Была изучена интерактивная сессия Zookeeper и было изучена работа приложения с барьерной синхронизацией.

Результат работы:

![Начало](https://github.com/DimaScientist/Big-Data/blob/main/LabWork4/images/animal_start.png)

![Окончание](https://github.com/DimaScientist/Big-Data/blob/main/LabWork4/images/animal_end.png)

## Упражнения:

### Задача об обедающих философах.

__Постановка задачи__:

*"Несколько безмолвных философов сидят вокруг круглого стола, перед каждым философом стоит тарелка спагетти. Вилки лежат на столе между каждой парой ближайших философов.
Каждый философ может либо есть, либо размышлять. Приём пищи не ограничен количеством оставшихся спагетти — подразумевается бесконечный запас. Тем не менее, философ может есть только тогда, когда держит две вилки — взятую справа и слева (альтернативная формулировка проблемы подразумевает миски с рисом и палочки для еды вместо тарелок со спагетти и вилок).
Каждый философ может взять ближайшую вилку (если она доступна) или положить — если он уже держит её. Взятие каждой вилки и возвращение её на стол являются раздельными действиями, которые должны выполняться одно за другим.
Вопрос задачи заключается в том, чтобы разработать модель поведения (параллельный алгоритм), при котором ни один из философов не будет голодать, то есть будет вечно чередовать приём пищи и размышления."*

__Решение__:


Заводим отдельный класс философов, который будет являться потоком:

```
class Philosopher(Thread)
```

Пусть каждыйт философ будет повторять свои действия несколько раз, к примеру 2 раза.

Решение заключается в использовании блокировок (__Lock__). Создадим высокоуровневую блокировку __lock__ для отслеживания всех блокировок:

```
lock = self.zk.Lock(f"{self.root}/heap", self.id)
```

И для отслеживания блокированных философов справа и слева:

```
left_fork = self.zk.Lock(f"{self.root}/{self.fork}/{self.left_bound}", self.id)
right_fork = self.zk.Lock(f"{self.root}/{self.fork}/{self.right_bound}", self.id)
```

До тех пор, пока философ продолжает свои действия, будем отслеживать состояние блокировок слева и справа. Если же претендентов на блокировку больше нет, то получаем блокировку. Если же соседние философы заблокированы, то посередине философ может поесть. После приема пищи блокировки снимаются.

```
with lock:
  if len(left_fork.contenders()) == 0 \
    and len(right_fork.contenders()) == 0:
    
    left_fork.acquire()
    right_fork.acquire()

  if right_fork.is_acquired and left_fork.is_acquired:
    
    logger.info(f"Philosopher {self.id} is eating")
    sleep(self.eating_time)
    left_fork.release()
    right_fork.release()
```

Данное решение в некотором роде реализует логику семафора.

Пример работы программы для 5 философов можно посмотреть в [логах](https://github.com/DimaScientist/Big-Data/blob/main/LabWork4/zookeeper-dining-philosophers/philosopher.log).


### Реализация протокола двухфазного коммита

__Постановка задачи__:

*"Протокол дфухфазного коммита (фиксации) транзакции - это алгоритм, который позволяет всем клиентам системы либо соглашатся на изменения (__commit__), либо откатывать их (__rollback__)."*

__Решение__ :

Основная часть решения взята с этого [ресурса](https://zookeeper.apache.org/doc/r3.4.2/recipes.html), более практически описано еще и [здесь](https://stackoverflow.com/questions/24635777/how-to-implement-2pc-in-zookeeper-cluster).

В соответствии с ним был создан класс __клиента__ (__Client__), представляющий отдельный поток. Этот поток записывал в свой эпиполярный узел одно из действий __commit/rollback__ (одно из них выбирается случайным образом). После чего при помощи декоратора __@self.zk.DataWatch__ подписываемся на отслеживание изменений в собственном узле. Если же версия данных в узле обновилась, то транзакция завершилась одним из исходов, выбранных __координатором__.

```
activity = b"commit" if randint(0, 10) > 5 else b"rollback"
self.zk.create(path=self.dir, value=activity, ephemeral=True)
logger.info(f"{str(activity.decode('utf-8'))} from client {self.id}")

@self.zk.DataWatch(self.dir)
def watch_node(data, stat):
  if stat.version > 0:
    logger.info(f"{data.decode('utf-8')} from client {self.id} has done")

sleep(self.waiting_time)
```

Был создан основной класс __координатор__ (__Coordinator__), который создает основной узел (*/app*) и узел для транзакций (*/app/tx*). Координатор подписывается на изменениях в дочерних узлах при помщи декоратора __@zk_coordinator.ChildrenWatch__ и ждет транзакции. После прихода изменений создается таймер для отслеживания тайм-аута, по истечению которого происходит голосование по принятию или отклонению транзакции, результат сообщается всем узлам-клиентам и таймер отменяется.

```
transaction_dir = f"{self.root_path}/tx"
zk_coordinator = self.zk
if zk_coordinator.connected is False:
  zk_coordinator.start()

def voting():
  children = zk_coordinator.get_children(transaction_dir)
  commit_transactions = 0
  rollback_transactions = 0
  if len(children) > 0:
    for child in children:
      node_info = zk_coordinator.get(f"{transaction_dir}/{child}")
      if node_info[0] == b'commit':
        commit_transactions += 1
      else:
        rollback_transactions += 1
    target_action = b"commit" if commit_transactions > rollback_transactions else b"rollback"
    for client in children:
      zk_coordinator.set(f"{transaction_dir}/{client}", target_action)
    
 @zk_coordinator.ChildrenWatch(transaction_dir)
 def children_watch(children):
  if self.timer is not None:
    self.timer.cancel()

  self.timer = Timer(interval=self.timeout, function=voting)
  self.timer.start()

  if len(children) < self.client_number:
    logger.info("Synchronizing all clients")
  elif len(children) == self.client_number:
    logger.info("Processing actions")
    self.timer.cancel()
    voting()
```

Пример работы можно посмотреть в следующем [журнале логов](https://github.com/DimaScientist/Big-Data/blob/main/LabWork4/zookeeper-two-phase-commit/two_phase_commit.log).
