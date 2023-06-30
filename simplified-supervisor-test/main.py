from common.supervisor.simplified_supervisor_process import SupervisorProcess
import common.supervisor.utils
import common.env_utils


def main():
    config = common.env_utils.read_config()

    supervisor_process = common.supervisor.utils.create_from_config(config)
    supervisor_process.run()


if __name__ == '__main__':
    main()
