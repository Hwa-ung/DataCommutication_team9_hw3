# HW#3 - 장애 허용 워커 링 (Fault-Tolerant Worker Ring)

9조

문서 수정일: 2026-10-26

## 1. 조원 정보

- 김대영 (20223087)
- 김민재 (20223089)
- 황태웅 (20223152)

## 2. 프로그램 개요

1부터 100,000,000까지의 소수 개수를 Worker 10개가 나누어 계산하는 프로그램이다. 계산 중 일부 Worker에 장애를 발생시키고, 남아 있는 Worker가 해당 작업을 이어받아 끝까지 처리하도록 구현했다.

Coordinator는 작업의 배정 상태와 결과를 관리하고, Worker는 계산 중간값을 다음 Worker에 저장한다. Launcher는 Worker 프로세스를 실행하고 CRASH와 RESTART 명령을 보내 장애와 복구를 발생시킨다. 세 구성요소는 TCP 소켓으로 통신한다.

- 전체 작업: 1,000개, 작업 번호는 0~999
- 작업 하나의 범위: 정수 100,000개
- 작업 하나의 계산 단계: 10단계, 단계마다 정수 10,000개
- 최종 소수 개수: 5,761,455
- 완료 조건: 모든 작업의 결과가 한 번씩만 확정되고 최종 합계가 일치해야 함

여기서 CRASH는 프로세스를 실제로 종료하는 것이 아니라 계산과 heartbeat를 멈추고 메모리의 작업 큐와 체크포인트를 비우는 방식이다. 따라서 RESTART도 기존 프로세스의 재참여로 처리한다.

프로그램 구성은 다음과 같다.

| 파일 | 내용 |
| --- | --- |
| coordinator.py | Worker 등록, 작업 배정, 장애 감지, 작업 인계, 결과 검증 |
| worker.py | 소수 계산, 체크포인트 송수신, 장애 및 재참여 처리 |
| launcher.py | Worker 10개 실행, 제어 연결, 장애 일정 관리, 프로세스 종료 확인 |
| tests/test_regressions.py | 상태 변경, 메시지 처리, 장애 복구에 대한 회귀 테스트 |
| tests/run_integration.py | Coordinator와 Worker 10개를 실행하는 전체 동작 테스트 |
| logs/ | 기존 실행에서 남긴 로그 |

## 3. 실행 환경 및 준비 사항

Python 표준 라이브러리만 사용하므로 별도로 pip 설치를 할 필요는 없다. 개발 테스트는 Windows와 Python 3.13에서 진행했다.

Coordinator는 EC2 또는 로컬 PC에서 실행하고, Launcher와 Worker 10개는 같은 로컬 PC에서 실행한다.

| 용도 | 주소 및 포트 |
| --- | --- |
| Coordinator 연결 | 실행할 때 지정한 IP와 TCP 포트. 아래 예시는 9000 사용 |
| Worker 간 통신 | TCP 7001~7010 |
| Launcher 제어 연결 | 같은 PC의 127.0.0.1:7001~7010 |

실행 전에 다음 사항을 확인한다.

- Python 실행 여부: python --version
- EC2 사용 시 현재 공인 IP, SSH 키 파일, SSH 접속 가능 여부
- Coordinator의 TCP 9000번 포트에 접속할 수 있는지
- 이전 실행이 Worker 포트 7001~7010을 사용하고 있지 않은지

모든 명령은 coordinator.py, worker.py, launcher.py가 있는 프로젝트 폴더에서 실행한다. 아래 Windows 예시는 python, EC2 예시는 python3를 사용한다.

## 4. EC2 준비 및 소스 전송

로컬 PC에서만 실행할 경우 이 부분을 건너뛰고 7항의 명령을 사용하면 된다.

EC2 보안 그룹에는 SSH용 TCP 22번과 Coordinator용 TCP 9000번 포트를 실행 PC에서 접속할 수 있도록 설정한다. Worker 간 통신과 Launcher 제어 연결은 로컬 PC에서 이루어지므로 EC2에 7001~7010번 포트를 열 필요는 없다.

로컬 PowerShell에서 실제 키 파일 경로와 현재 공인 IP를 넣는다. 새 PowerShell 창을 열면 변수도 다시 설정한다.

~~~powershell
$Ec2Key = "<SSH 키 파일의 전체 경로>"
$Ec2Ip = "<현재 EC2 공인 IP>"
~~~

Coordinator 소스를 EC2에 전송한다.

~~~powershell
ssh -i "$Ec2Key" "ubuntu@$Ec2Ip" "mkdir -p ~/hw3"
scp -i "$Ec2Key" ./coordinator.py "ubuntu@${Ec2Ip}:~/hw3/"
~~~

코드를 수정한 뒤에는 EC2의 coordinator.py도 다시 전송해야 한다. 로컬 Worker와 EC2 Coordinator는 같은 버전으로 실행한다.

## 5. 명령행 인자

### Coordinator

| 인자 | 내용 |
| --- | --- |
| --host | 연결을 받을 주소. 기본값 0.0.0.0 |
| --port | 연결을 받을 TCP 포트. 필수 |
| --log-dir | 로그 저장 폴더. 기본값 logs |

### Launcher

| 인자 | 내용 |
| --- | --- |
| --coord-ip | Coordinator IP. 필수 |
| --coord-port | Coordinator 포트. 필수 |
| --seed | 장애 대상과 시점에 사용하는 난수 seed. 기본값 42 |
| --step-delay | Worker의 계산 단계별 지연 시간(ms). 기본값 100 |
| --peer-ip | Worker가 다른 Worker에 알려줄 IP. 기본값 127.0.0.1 |
| --log-dir | Launcher와 Worker의 로그 저장 폴더. 기본값 logs |

Worker는 Launcher가 자동으로 실행하므로 따로 10개를 실행할 필요는 없다. Worker를 직접 실행할 때는 --id에 1~10 중 하나를 지정해야 한다.

--wait-for-launcher는 Launcher가 Worker를 실행할 때 자동으로 붙이는 옵션이다. 이 옵션이 있으면 Worker는 Launcher의 제어 연결을 받은 뒤 Coordinator에 등록한다. Worker를 직접 실행할 때 이 옵션을 넣으면 Launcher가 연결될 때까지 기다리므로, 제어 연결 없이 실행할 경우에는 생략한다.

## 6. 프로그램 실행 (EC2 + 로컬 PC)

같은 로그 폴더로 다시 실행하면 이전 로그를 덮어쓴다. 실행 결과를 보관하려면 run-01, run-02처럼 매번 다른 폴더를 지정한다.

### 6-1. Coordinator 실행

먼저 로컬 PowerShell에서 EC2에 접속한다.

~~~powershell
ssh -i "$Ec2Key" "ubuntu@$Ec2Ip"
~~~

EC2 터미널에서 실행한다.

~~~sh
cd ~/hw3
python3 coordinator.py --host 0.0.0.0 --port 9000 --log-dir logs/run-01
~~~

INIT 로그가 출력되면 Worker 연결을 기다리는 상태이다.

### 6-2. 연결 확인 및 Launcher 실행

새 로컬 PowerShell 창을 열고 프로젝트 폴더로 이동한다. 새 창에서는 Ec2Ip 변수를 다시 지정한다.

~~~powershell
$Ec2Ip = "<현재 EC2 공인 IP>"
Test-NetConnection $Ec2Ip -Port 9000
~~~

TcpTestSucceeded가 True이면 Launcher를 실행한다.

~~~powershell
python launcher.py --coord-ip "$Ec2Ip" --coord-port 9000 --seed 42 --step-delay 100 --log-dir logs/run-01
~~~

Launcher가 Worker 10개를 실행한 후 제어 연결을 맺는다. Worker들이 Coordinator에 등록하고 링 연결을 마치면 계산을 시작한다. 모든 Worker의 READY를 받은 시점부터 Launcher의 장애 일정이 시작된다.

### 6-3. 완료 확인

Coordinator 로그에서 다음 항목을 확인한다.

~~~text
[VERIFY] total=5761455 all_done=True all_commit_once=True double=0 ...
[VERIFY] PASS
~~~

위 출력에서 ...는 실행 시간 등 뒤에 붙는 항목을 생략한 것이다.

작업이 모두 완료되면 Coordinator가 BYE를 보내고 Worker와 Launcher가 종료된다. 정상 완료 시 종료 코드는 0이다. Coordinator의 최종 검증 실패나 Launcher가 감지한 Worker 실행 실패는 종료 코드 1로 보고한다.

## 7. 로컬 실행 및 테스트

### 7-1. 터미널 두 개로 실행

터미널 1에서 Coordinator를 먼저 실행한다.

~~~powershell
python coordinator.py --host 127.0.0.1 --port 9000 --log-dir logs/local-01
~~~

터미널 2에서 Launcher를 실행한다.

~~~powershell
python launcher.py --coord-ip 127.0.0.1 --coord-port 9000 --seed 42 --step-delay 100 --log-dir logs/local-01
~~~

### 7-2. 회귀 테스트

~~~powershell
python -m unittest discover -s tests -v
~~~

2026-10-26 기준 28개 테스트를 통과했다. 잘못된 작업 번호, 중복 결과, 장애 중 배정, 연결 종료, READY 중복 수신, CRASH와 송신의 순서 등을 확인한다.

### 7-3. 자동 실행 테스트

아래 명령은 Coordinator와 Launcher를 자동으로 실행한다. Worker 포트가 고정되어 있으므로 하나가 끝난 다음에 다음 명령을 실행한다.

~~~powershell
python tests/run_integration.py --step-delay 0
python tests/run_integration.py --step-delay 100 --seed 42
python tests/run_integration.py --step-delay 100 --seed 7
python tests/run_integration.py --step-delay 100 --disconnect-coordinator
~~~

--step-delay 0은 전체 계산을 빠르게 확인하는 용도이다. 첫 장애가 발생하기 전에 계산이 끝날 수 있으므로 장애 복구 확인에는 --step-delay 100을 사용한다. 기본 제한 시간은 240초이며, 필요한 경우 --timeout으로 조정할 수 있다.

--disconnect-coordinator는 실행 도중 Coordinator를 종료하는 테스트이다. 이 경우에는 Launcher가 종료 코드 1을 반환하고 남은 Worker를 정리해야 통과한다.

자동 테스트의 로그는 실행할 때 출력되는 .test-runs/ 아래의 개별 폴더에 저장된다.

## 8. 주요 구현 내용

### 8-1. 작업 배정과 결과 확정

Coordinator에는 Listener 1개, Dispatcher 6개, 장애 감지 스레드 1개가 있다. Listener가 받은 일반 요청은 요청 큐에 넣고 Dispatcher가 꺼내 처리한다. Heartbeat는 요청 큐에서 기다리지 않도록 Listener가 바로 반영한다.

아직 배정하지 않은 작업은 pending 큐에 저장한다. Worker가 REQ를 보내면 작업 하나를 꺼내 ASSIGN으로 전달한다. 각 작업에는 상태, 담당 Worker, epoch, 결과, 확정 횟수를 저장한다.

작업 상태는 PENDING, ASSIGNED, DONE으로 구분한다. 결과를 받으면 현재 담당 Worker와 epoch가 맞는지, 이미 완료한 작업인지 확인한 뒤 확정한다. 작업을 다시 계산하는 경우가 있더라도 최종 결과는 한 번만 반영한다.

### 8-2. Worker 링과 체크포인트

Worker 1은 2에게, 2는 3에게 체크포인트를 보내며 Worker 10은 1에게 보낸다. 체크포인트에는 작업 번호, epoch, 완료한 단계, 그 단계까지 구한 소수 개수를 넣는다.

각 Worker는 작업의 1~9단계가 끝날 때 CKPT를 보내고, 10단계가 끝나면 Coordinator에 RESULT를 보낸다. 작업을 인계받은 Worker에 사용할 수 있는 체크포인트가 있으면 그 다음 단계부터 계산한다. 없으면 첫 단계부터 다시 계산한다.

예를 들어 5단계까지의 체크포인트가 있으면 저장된 소수 개수에 6~10단계의 결과를 더한다. 이미 계산한 1~5단계를 다시 더하지 않는다.

### 8-3. 장애 감지와 작업 인계

Worker는 실행 중 약 1초 간격으로 HB를 보낸다. Coordinator는 1초마다 마지막 수신 시각을 확인하고, 3초 이상 지나면 SUSPECT, 5초 이상 지나면 DEAD로 바꾼다.

DEAD로 판정한 Worker의 미완료 작업은 링 순서상 다음 ALIVE Worker에 넘긴다. ALIVE가 없으면 SUSPECT를 찾고, 받을 Worker가 전혀 없으면 다음 검사에서 다시 확인한다.

인계할 때는 담당 Worker를 바꾸고 epoch를 1 증가시킨다. epoch는 같은 작업이 몇 번째 배정인지 구분하는 번호이다. 이전 Worker가 늦게 보낸 결과는 현재 담당자와 epoch 검사에서 거절한다.

### 8-4. 장애 주입과 재참여

Launcher는 모든 Worker의 READY를 받은 뒤 10~20초 사이에 첫 장애를 발생시킨다. 이후 장애 간격은 5~15초이며, 장애 대상은 실행 중인 Worker 중에서 고른다.

- 장애 상태 또는 재참여 중인 Worker는 동시에 최대 2개로 제한한다.
- CRASH 후 10~15초가 지나면 해당 Worker에 RESTART를 보낸다.
- 시작 후 80초가 지나면 새로운 장애는 발생시키지 않는다.
- 이미 예정된 RESTART는 80초가 지난 뒤에도 처리한다.

RESTART를 받은 Worker는 Coordinator에 REJOIN을 보낸다. Coordinator가 아직 DEAD로 판정하지 않았다면 RETRY_LATER를 받고 2초 후 다시 요청한다. WELCOME을 받으면 계산과 heartbeat를 재개한다.

### 8-5. 통신 형식

메시지는 UTF-8 문자열 한 줄로 보내며 끝에 LF를 붙인다. TCP 수신 한 번과 메시지 하나가 항상 일치하지 않으므로, 수신 버퍼에 쌓은 뒤 줄 단위로 나누어 처리한다.

주요 메시지 형식은 다음과 같다.

~~~text
REQ <worker_id>
ASSIGN <task_id> <epoch>
CKPT <task_id> <epoch> <step> <partial_count>
RESULT <task_id> <epoch> <prime_count>
TAKEOVER <task_id> <new_epoch> <previous_worker_id>
REJOIN <worker_id>
~~~

RESULT를 확정하면 ACK를, 이전 배정의 결과이거나 이미 완료한 작업이면 REJECT_STALE을 보낸다. 모든 작업이 완료되면 BYE로 종료를 알린다.

## 9. 로그 확인 및 실행 결과

실행별 로그는 Coordinator의 coordinator.log, Launcher의 launcher.log, Worker별 worker_1.log~worker_10.log로 나뉜다. EC2에서 실행한 coordinator.log는 EC2에 있고, 나머지 로그는 로컬 PC에 있다.

EC2 로그를 가져올 때는 로컬 PowerShell에서 4항의 Ec2Key와 Ec2Ip를 설정하고 같은 실행의 로그 폴더를 지정한다.

~~~powershell
scp -i "$Ec2Key" "ubuntu@${Ec2Ip}:~/hw3/logs/run-01/coordinator.log" ./logs/run-01/
~~~

로그에서는 다음 내용을 확인한다.

- ASSIGN, COMMIT: 작업 배정과 결과 확정
- FD, TAKEOVER, REJOIN: 장애 판정, 작업 인계, 재참여
- REJECT: 이전 epoch 또는 이미 완료한 작업의 결과 거절
- POOL: 5초마다 기록하는 대기 작업, 완료 작업, Worker 상태
- VERIFY, TERMINATE: 최종 검증과 종료 통계
- Launcher의 EVENT: 장애 대상, 중단 시각, 재시작 시각, 실제 재참여 시각

로그 시각은 실행한 PC의 시스템 시각을 사용한다. 서로 다른 PC의 로그를 비교할 때는 시간대도 확인한다. 장애 판정과 실행 시간 계산에는 time.monotonic()을 사용한다.

### 실행 결과 기록

아래는 Windows, Python 3.13에서 기록한 로컬 테스트 결과이다.

| 날짜 | 실행 조건 | 결과 |
| --- | --- | --- |
| 10월 24일 | --step-delay 0 | 1,000개 완료, 합계 5,761,455, 중복 확정 0개 |
| 10월 24일 | --step-delay 100 --seed 42 | 119.9초, DEAD 8회 / TAKEOVER 7건 / REJOIN 8회, PASS |
| 10월 24일 | --step-delay 100 --seed 7 | 118.8초, DEAD 8회 / TAKEOVER 8건 / REJOIN 8회, PASS |
| 10월 24일 | Coordinator 연결 강제 종료 | Worker 종료 및 Launcher 종료 코드 1 확인 |
| 10월 26일 | 시작 순서 수정 후 --step-delay 0 | 계산 0.6초, 전체 작업 완료 및 정상 종료 |
| 10월 26일 | 수정 후 --step-delay 100 --seed 42 | 121.9초, DEAD 8회 / TAKEOVER 8건 / REJOIN 8회, PASS |
| 10월 26일 | 회귀 테스트 | 28개 통과 |

10월 26일 장애 주입 실행에서도 1,000개 작업이 각각 한 번 확정되었고, 합계는 5,761,455였다. Coordinator와 Launcher는 모두 종료 코드 0으로 끝났다.

장애 횟수와 인계 작업 수는 항상 같지는 않다. 장애가 발생한 시점에 담당하던 미완료 작업이 없으면 넘길 작업도 없다. 실행 시간과 작업 배정은 PC 성능이나 스레드 실행 순서에 따라 달라질 수 있다.

저장소의 기존 logs/는 이전 실행 기록이다. 새 실행 결과와 비교할 때는 같은 실행에서 나온 파일끼리 확인한다.

## 10. 발견한 문제와 수정 내용

### 10-1. 최초 점검에서 수정한 내용 (2026-10-24)

**결과 저장과 장애 판정이 동시에 진행되는 문제**

RESULT 처리에서 Worker가 DEAD인지 확인한 다음 잠금을 풀고 작업 결과를 저장하고 있었다. 그 사이에 장애 감지 스레드가 상태를 바꾸면 앞에서 확인한 상태와 실제 저장 시점의 상태가 달라질 수 있었다. Worker 상태 확인부터 작업의 담당자와 epoch 확인, DONE 전환까지 같은 잠금 구간에서 처리하도록 수정했다.

**인계할 Worker가 없을 때 작업이 남는 문제**

장애가 난 순간에만 인계 대상을 찾으면, 그때 받을 Worker가 없었던 작업은 이후에도 남아 있을 수 있었다. 장애 검사마다 DEAD Worker 소유의 미완료 작업을 다시 확인하도록 바꿨다. 재참여한 Worker에 이전 작업이 남아 있는 경우에는 epoch를 올려 다시 전달한다.

**CRASH 처리와 작업 큐 변경이 겹치는 문제**

CRASH 상태 전환과 큐 초기화가 따로 이루어져 초기화 중에 배정이나 체크포인트가 들어올 여지가 있었다. 상태 변경과 큐·체크포인트 초기화를 함께 보호하고, 수신 처리에서도 Worker 상태를 다시 확인하게 했다. 중복 RESTART나 늦게 도착한 WELCOME도 현재 상태가 맞을 때만 처리하도록 했다.

**작업 번호와 수신 메시지 검사 부족**

음수 작업 번호가 들어오면 Python의 음수 인덱스로 마지막 작업을 가리킬 수 있었다. 작업 번호, epoch, 결과값의 범위를 검사하고, 요청에 적힌 Worker ID가 실제 등록된 연결과 일치하는지도 확인하도록 했다. 빈 줄이나 숫자가 아닌 CKPT 필드가 들어와도 수신 스레드가 종료되지 않도록 처리했다. 인계 시에는 현재 epoch보다 이전인 유효 체크포인트만 사용한다.

**연결 종료 후에도 스레드가 기다리는 문제**

Coordinator와의 연결이 끊겨도 계산 또는 heartbeat 스레드가 대기 상태에 남을 수 있었다. 연결 종료나 Coordinator 송신 실패를 확인하면 종료 상태를 설정하고 대기 중인 스레드를 모두 깨우도록 했다. 계산 단계 사이의 지연도 종료 요청을 받으면 중단할 수 있게 했다.

**Launcher의 READY 집계와 실패 시 정리 문제**

READY 메시지 수만 세면 같은 Worker의 메시지가 중복 집계될 수 있고, READY를 기다리는 중 연결이 끊겨도 대기가 끝나지 않았다. Worker ID별로 한 번만 집계하고, 60초 제한과 연결 종료 처리를 추가했다. 실행 실패나 중단 시에는 Launcher가 시작한 프로세스와 소켓을 정리한다.

**송신 실패, 시간 측정, 로그 종료 처리**

Coordinator의 비차단 sendall()이 실패하면 메시지 일부만 전송된 상태가 남을 수 있었다. 송신에 제한 시간을 두고, 실패한 연결은 종료해 장애 감지와 작업 회수로 이어지게 했다. Launcher도 제어 메시지 송신 실패를 오류로 처리한다. 경과 시간은 시스템 시각 변경의 영향을 받지 않도록 time.monotonic()으로 계산하고, 로그 기록과 파일 닫기에는 같은 잠금을 사용한다.

Worker ID와 음수 지연 시간은 실행 전에 검사한다. 연결 재시도에서 실패한 소켓도 닫도록 수정했으며, 자동 테스트 결과 폴더는 Git 추적에서 제외했다.

### 10-2. 추가 실행에서 수정한 내용 (2026-10-26)

**빠른 계산에서 Launcher가 제어 연결을 만들지 못하는 문제**

--step-delay 0으로 실행하면 계산이 약 1초 안에 끝날 수 있었다. Launcher는 Worker를 실행한 뒤 1초를 기다리는데, 그 사이에 Worker가 계산을 끝내고 종료하면 제어 연결을 맺지 못했다.

Launcher가 실행하는 Worker에 --wait-for-launcher를 전달하도록 바꿨다. Worker는 제어 연결을 받은 뒤 Coordinator에 등록하므로, Launcher가 연결되기 전에 계산이 끝나는 상황을 막는다.

**CRASH 상태로 바뀐 뒤에도 메시지가 전송될 수 있는 문제**

기존 send_if_alive()는 상태를 확인한 뒤 잠금을 풀고 전송했다. 확인 직후 CRASH가 들어오면 이미 장애 상태로 바뀐 뒤 HB, CKPT 또는 RESULT가 나갈 수 있었다.

송신과 CRASH 전환이 같은 runtime_send_lock을 사용하게 했다. 진행 중인 전송은 먼저 끝나고 CRASH가 반영되며, CRASH 반영 후에는 이전 실행 상태의 메시지를 보내지 않는다. 상태·큐·체크포인트 잠금은 네트워크 전송 전에 해제한다.

**TAKEOVER가 NO_TASK 상태를 해제하는 문제**

Worker가 NO_TASK를 받으면 새로 배정받을 작업이 없다는 뜻으로 no_task를 설정한다. 그런데 TAKEOVER를 받을 때 이 값을 다시 False로 바꾸고 있어, 인계받은 작업을 처리한 뒤 불필요한 REQ를 보내게 되었다.

TAKEOVER를 받아도 no_task는 유지하도록 수정했다. 새 작업을 요청하지 않는 상태에서도 로컬 큐에 들어온 인계 작업은 처리할 수 있다. CRASH와 송신의 순서, TAKEOVER 후 no_task 유지 여부를 확인하는 테스트 2개도 추가했다.

## 11. 실행 중 문제가 생긴 경우

**Coordinator에 연결되지 않을 때**

Coordinator가 먼저 실행되었는지, IP와 포트가 맞는지 확인한다. EC2를 사용하면 현재 공인 IP와 보안 그룹의 TCP 9000번 규칙을 확인한다.

**Address already in use가 나올 때**

이전 실행이 아직 해당 포트를 사용하고 있는지 확인한다. Worker 포트가 7001~7010으로 고정되어 있으므로 같은 PC에서 여러 실행을 동시에 진행하지 않는다.

**READY 대기 또는 제어 연결 오류가 날 때**

Worker 로그에서 INIT과 LAUNCHER 기록을 확인한다. Launcher와 Worker가 같은 버전인지 확인하고, Worker를 직접 실행했다면 --wait-for-launcher 옵션을 넣었는지도 확인한다.

**VERIFY가 FAIL이거나 프로그램이 오류로 종료될 때**

Coordinator의 VERIFY와 ERROR, Launcher의 ERROR, Worker별 종료 기록을 확인한다. 연결 강제 종료 테스트가 아니라면 정상 완료로 보지 않고 원인을 확인한 뒤 다시 실행한다.

**실제 Worker 프로세스가 종료된 경우**

현재 프로그램은 모의 CRASH/RESTART를 대상으로 하므로 종료된 Worker 프로세스를 자동으로 새로 만들지는 않는다. Launcher가 실제 프로세스 오류를 감지하면 남은 Worker를 정리하고 실패를 보고한다. Coordinator의 메모리 상태를 저장했다가 복구하는 기능도 구현하지 않았다.
