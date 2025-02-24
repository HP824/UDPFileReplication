#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <ctime>

using namespace std;

#define dprintf printf

constexpr uint16_t CMD_PUT = 1;
constexpr uint16_t CMD_DATA = 2;
constexpr uint16_t CMD_ACK = 3;
constexpr uint16_t CMD_ERROR = 4;

constexpr size_t MAX_PACKET_SIZE = 4096;

std::string currentRFC3339Time() {
    std::time_t t_now = std::time({});
    char rfc_time[24];
    std::strftime(std::data(rfc_time), std::size(rfc_time),
                  "%FT%TZ", std::gmtime(&t_now));
	return rfc_time;
}

int setRecvTimeout(int sockfd, time_t seconds) {
  timeval timeout{};
  timeout.tv_sec = seconds;
  timeout.tv_usec = 0;

  if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) <
      0) {
    perror("Error setting socket timeout");
    return -1;
  }

  return 0;
}

int checkFileAccess(const char *file_path) {
  struct stat sb;

  if (stat(file_path, &sb) != 0) {
    // dprintf("Creating file: %s\n", file_path);
    char dir_path[1024];
    strcpy(dir_path, file_path);
    char *slash_ptr = strrchr(dir_path, '/');
    *slash_ptr = '\0';

    struct stat sd;
    if (stat(dir_path, &sd) != 0) {
      dprintf("directory does not exist: %s\n", dir_path);
      mkdir(dir_path, 0777);
    } else {
      if (!S_ISDIR(sd.st_mode)) {
        dprintf("path is not dir: %s\n", dir_path);
        return -1;
      }
    }
  } else {
    if (!S_ISREG(sb.st_mode)) {
      dprintf("Path is not a file: %s\n", file_path);
      return -1;
    }
  }
  return 0;
}

int main(int argc, char *argv[]) {
  if (argc != 4) {
    std::cerr << "Usage: ./myserver <port> <droppc> <root_folder_path>" << std::endl;
    return EXIT_FAILURE;
  }

  int port = std::stoi(argv[1]);
  int droppc = std::stoi(argv[2]);
  std::string root_path = argv[3];

  if(root_path.back() != '/') {
  	root_path.push_back('/');
  }

  if (droppc < 0 || droppc > 100) {
    perror("Invalid droppc");
    return EXIT_FAILURE;
  }

  uint32_t seed = arc4random();
  srand(seed);

  int server_socket = socket(AF_INET, SOCK_DGRAM, 0);
  if (server_socket < 0) {
    perror("Error creating socket");
    return EXIT_FAILURE;
  }

  sockaddr_in server_addr{};
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  if (bind(server_socket, (struct sockaddr *)&server_addr,
           sizeof(server_addr)) < 0) {
    perror("Error binding socket");
    close(server_socket);
    return EXIT_FAILURE;
  }

  std::cout << "Server listening on port " << port << "..." << std::endl;

  while (true) {
    std::vector<char> recv_buffer(MAX_PACKET_SIZE);
    std::vector<char> send_buffer(MAX_PACKET_SIZE);
    std::unordered_map<uint32_t, std::vector<char>> received_chunks;
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);

    uint32_t sequence_number = 0;
    std::string output_file_path;

    dprintf("Waiting for client...\n");
    setRecvTimeout(server_socket, 0);
    while (true) {
      ssize_t bytes_received =
          recvfrom(server_socket, recv_buffer.data(), MAX_PACKET_SIZE, 0,
                   (struct sockaddr *)&client_addr, &client_len);
      if (bytes_received < 0) {
        perror("Error receiving packet");
        break;
      }

	  char *cli_ip = inet_ntoa(client_addr.sin_addr);
	  int cli_port = ntohs(client_addr.sin_port);

      // dprintf("Bytes received: %ld\n", bytes_received);

      uint16_t cmd;
      memcpy(&cmd, recv_buffer.data(), sizeof(uint16_t));

      uint32_t received_seq_num;
      if (cmd == CMD_PUT) {
        received_seq_num = 0;
		std::string output_file_name;
		output_file_name.assign(recv_buffer.data() + sizeof(uint16_t),
                                bytes_received - sizeof(uint16_t));
		output_file_path = root_path + output_file_name;
        setRecvTimeout(server_socket, 10);
      } else if (cmd == CMD_DATA) {
        // Extract sequence number from the response
        memcpy(&received_seq_num, recv_buffer.data() + sizeof(uint16_t),
               sizeof(uint32_t));

        int random = rand() % 100;
        if (random < droppc) {
          std::cout << currentRFC3339Time() << ", " << port << ", " << cli_ip
			  << ", " << cli_port << ", DROP DATA, " << received_seq_num << std::endl;
          continue;
        }

        std::cout << currentRFC3339Time() << ", " << port << ", " << cli_ip
			  << ", " << cli_port  << ", DATA, " << received_seq_num << std::endl;
        if (bytes_received > (ssize_t)(sizeof(uint16_t) + sizeof(uint32_t))) {
          received_chunks[received_seq_num] = std::vector<char>(
              recv_buffer.begin() + sizeof(uint16_t) + sizeof(uint32_t),
              recv_buffer.begin() + bytes_received);
          sequence_number++;
        }
      }

      uint16_t ack_cmd = CMD_ACK;

	  int random2 = rand() % 100;
	  if (random2 < droppc) {
	    std::cout << currentRFC3339Time() << ", " << port << ", " << cli_ip
			  << ", " << cli_port << ", DROP ACK, " << received_seq_num << std::endl;
	    continue;
	  }

	  std::cout << currentRFC3339Time() << ", " << port << ", " << cli_ip
			  << ", " << cli_port << ", ACK, " << received_seq_num << std::endl;
      memcpy(send_buffer.data(), &ack_cmd, sizeof(uint16_t));
      memcpy(send_buffer.data() + sizeof(uint16_t), &received_seq_num,
             sizeof(uint32_t));

      ssize_t bytes_sent = sendto(server_socket, send_buffer.data(),
                                  sizeof(uint16_t) + sizeof(uint32_t), 0,
                                  (struct sockaddr *)&client_addr, client_len);
      if (bytes_sent < 0) {
        // printf("%ld %d\n", bytes_sent, errno);
        perror("Error sending packet");
      }

      if (cmd == CMD_DATA &&
          bytes_received == sizeof(uint16_t) + sizeof(uint32_t))
        break;
    }

    if (checkFileAccess(output_file_path.c_str()) != 0)
      continue;
    // Write reassembled file
    std::ofstream output_file(output_file_path);
    if (!output_file.is_open()) {
      std::cerr << "Error: Cannot open output file '" << output_file_path
                << "'." << std::endl;
      continue;
    }

    for (uint32_t i = 0; i < sequence_number; ++i) {
      if (received_chunks.find(i) == received_chunks.end()) {
        std::cerr << "Packet loss detected: " << i << std::endl;
        break;
      }
      // dprintf("chunk size: %ld\n", received_chunks[i].size());
      // dprintf("%c%c\n", received_chunks[i][0], received_chunks[i].at(1));
      output_file.write(received_chunks[i].data(), received_chunks[i].size());
    }

    output_file.close();
  }
  close(server_socket);

  return EXIT_SUCCESS;
}
