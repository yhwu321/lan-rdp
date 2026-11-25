#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use enigo::{Button, Coordinate, Direction, Enigo, Key, Keyboard, Mouse, Settings};
use image::codecs::jpeg::JpegEncoder;
use image::ColorType;
use local_ip_address::local_ip;
use scrap::{Capturer, Display};
use serde::{Deserialize, Serialize};
use std::io::ErrorKind::WouldBlock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

// ==========================================
// 1. 网络通信协议定义
// ==========================================
#[derive(Serialize, Deserialize, Debug, Clone)]
enum NetPacket {
    /// 屏幕图像帧 (宽, 高, JPEG字节流)
    ScreenFrame {
        width: u32,
        height: u32,
        jpeg_data: Vec<u8>,
    },
    /// 鼠标移动 (归一化坐标 0.0 ~ 1.0)
    MouseMove { x_ratio: f32, y_ratio: f32 },
    /// 鼠标按键
    MouseButton { button: MouseBtn, down: bool },
    /// 鼠标滚轮
    MouseScroll { delta_y: i32 },
    /// 键盘按键
    KeyInput { key_char: Option<char>, special_key: Option<SpecialKey>, down: bool },
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
enum MouseBtn {
    Left,
    Right,
    Middle,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
enum SpecialKey {
    Enter,
    Backspace,
    Tab,
    Escape,
    Space,
    Up,
    Down,
    Left,
    Right,
    Delete,
}

/// 长度前缀帧写入
async fn send_packet<W: AsyncWriteExt + Unpin>(writer: &mut W, packet: &NetPacket) -> std::io::Result<()> {
    let payload = bincode::serialize(packet).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

/// 长度前缀帧读取
async fn recv_packet<R: AsyncReadExt + Unpin>(reader: &mut R) -> std::io::Result<NetPacket> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 20 * 1024 * 1024 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Packet too large"));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    bincode::deserialize(&payload).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}

// ==========================================
// 2. 被控端逻辑 (服务端：推流屏幕 + 执行键鼠)
// ==========================================
async fn run_host_server(port: u16, stop_flag: Arc<AtomicBool>, status: Arc<Mutex<String>>) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => {
            *status.lock().unwrap() = format!("等待连接 (监听端口: {})", port);
            l
        }
        Err(e) => {
            *status.lock().unwrap() = format!("端口 {} 绑定失败: {}", port, e);
            return;
        }
    };

    loop {
        if stop_flag.load(Ordering::Relaxed) {
            break;
        }

        // 使用 timeout 以便定期检查端口是否被用户修改
        let accept_res = tokio::time::timeout(Duration::from_millis(500), listener.accept()).await;
        if let Ok(Ok((stream, peer_addr))) = accept_res {
            *status.lock().unwrap() = format!("正在被 {} 远程控制", peer_addr);
            let status_clone = status.clone();
            tokio::spawn(async move {
                handle_host_client(stream).await;
                *status_clone.lock().unwrap() = format!("等待连接 (监听端口: {})", port);
            });
        }
    }
}

async fn handle_host_client(stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();
    let session_active = Arc::new(AtomicBool::new(true));

    // 任务 A: 独立线程捕获屏幕并编码为 JPEG 发送
    let active_tx = session_active.clone();
    let (frame_tx, mut frame_rx) = mpsc::channel::<NetPacket>(2);

    std::thread::spawn(move || {
        let display = match Display::primary() {
            Ok(d) => d,
            Err(_) => return,
        };
        let mut capturer = match Capturer::new(display) {
            Ok(c) => c,
            Err(_) => return,
        };
        let width = capturer.width() as u32;
        let height = capturer.height() as u32;

        while active_tx.load(Ordering::Relaxed) {
            match capturer.frame() {
                Ok(buffer) => {
                    // scrap 返回 BGRA 格式，转换为 RGB 供 JPEG 编码
                    let stride = buffer.len() / height as usize;
                    let mut rgb = Vec::with_capacity((width * height * 3) as usize);
                    for y in 0..height as usize {
                        for x in 0..width as usize {
                            let i = y * stride + x * 4;
                            rgb.push(buffer[i + 2]); // R
                            rgb.push(buffer[i + 1]); // G
                            rgb.push(buffer[i]);     // B
                        }
                    }

                    let mut jpeg_data = Vec::new();
                    let mut encoder = JpegEncoder::new_with_quality(&mut jpeg_data, 65);
                    if encoder.encode(&rgb, width, height, ColorType::Rgb8.into()).is_ok() {
                        let pkt = NetPacket::ScreenFrame {
                            width,
                            height,
                            jpeg_data,
                        };
                        // 如果发送队列满则丢弃旧帧，保证实时性不堆积延迟
                        let _ = frame_tx.try_send(pkt);
                    }
                    std::thread::sleep(Duration::from_millis(33)); // 约 30 FPS
                }
                Err(ref e) if e.kind() == WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
    });

    // 异步发送屏幕帧
    let active_send = session_active.clone();
    let send_task = tokio::spawn(async move {
        while let Some(pkt) = frame_rx.recv().await {
            if send_packet(&mut writer, &pkt).await.is_err() {
                break;
            }
        }
        active_send.store(false, Ordering::Relaxed);
    });

    // 任务 B: 接收主控端键鼠事件并在本机重放
    let active_recv = session_active.clone();
    let recv_task = tokio::task::spawn_blocking(move || {
        let mut enigo = match Enigo::new(&Settings::default()) {
            Ok(e) => e,
            Err(_) => return,
        };
        let rt = tokio::runtime::Handle::current();
        while active_recv.load(Ordering::Relaxed) {
            let pkt = match rt.block_on(recv_packet(&mut reader)) {
                Ok(p) => p,
                Err(_) => break,
            };

            match pkt {
                NetPacket::MouseMove { x_ratio, y_ratio } => {
                    if let Ok((w, h)) = enigo.main_display() {
                        let tx = (x_ratio.clamp(0.0, 1.0) * w as f32) as i32;
                        let ty = (y_ratio.clamp(0.0, 1.0) * h as f32) as i32;
                        let _ = enigo.move_mouse(tx, ty, Coordinate::Abs);
                    }
                }
                NetPacket::MouseButton { button, down } => {
                    let btn = match button {
                        MouseBtn::Left => Button::Left,
                        MouseBtn::Right => Button::Right,
                        MouseBtn::Middle => Button::Middle,
                    };
                    let dir = if down { Direction::Press } else { Direction::Release };
                    let _ = enigo.button(btn, dir);
                }
                NetPacket::MouseScroll { delta_y } => {
                    let _ = enigo.scroll(delta_y, enigo::Axis::Vertical);
                }
                NetPacket::KeyInput { key_char, special_key, down } => {
                    let dir = if down { Direction::Press } else { Direction::Release };
                    if let Some(ch) = key_char {
                        let _ = enigo.key(Key::Unicode(ch), dir);
                    } else if let Some(sk) = special_key {
                        let k = match sk {
                            SpecialKey::Enter => Key::Return,
                            SpecialKey::Backspace => Key::Backspace,
                            SpecialKey::Tab => Key::Tab,
                            SpecialKey::Escape => Key::Escape,
                            SpecialKey::Space => Key::Space,
                            SpecialKey::Up => Key::UpArrow,
                            SpecialKey::Down => Key::DownArrow,
                            SpecialKey::Left => Key::LeftArrow,
                            SpecialKey::Right => Key::RightArrow,
                            SpecialKey::Delete => Key::Delete,
                        };
                        let _ = enigo.key(k, dir);
                    }
                }
                _ => {}
            }
        }
        active_recv.store(false, Ordering::Relaxed);
    });

    let _ = tokio::join!(send_task, recv_task);
}

// ==========================================
// 3. GUI 主控端与界面逻辑
// ==========================================
struct RemoteDesktopApp {
    local_ip_str: String,
    listen_port_input: String,
    current_listen_port: u16,
    host_stop_flag: Arc<AtomicBool>,
    host_status: Arc<Mutex<String>>,

    target_addr_input: String,
    client_status: Arc<Mutex<String>>,
    is_connected: Arc<AtomicBool>,

    /// 最新接收到的远程桌面画面 (RGBA, width, height)
    latest_frame: Arc<Mutex<Option<(Vec<u8>, usize, usize)>>>,
    remote_texture: Option<egui::TextureHandle>,

    /// 发送键鼠指令到远程的通道
    input_tx: Option<mpsc::UnboundedSender<NetPacket>>,
    is_fullscreen: bool,
    rt_handle: tokio::runtime::Handle,
}

impl RemoteDesktopApp {
    fn new(cc: &eframe::CreationContext<'_>, rt_handle: tokio::runtime::Handle) -> Self {
        // 加载中文字体以防乱码 (优先加载 Windows 微软雅黑)
        let mut fonts = egui::FontDefinitions::default();
        if let Ok(font_data) = std::fs::read("C:\\Windows\\Fonts\\msyh.ttc") {
            fonts.font_data.insert(
                "msyh".to_owned(),
                egui::FontData::from_owned(font_data),
            );
            fonts
                .families
                .entry(egui::FontFamily::Proportional)
                .or_default()
                .insert(0, "msyh".to_owned());
        }
        cc.egui_ctx.set_fonts(fonts);

        let ip = local_ip()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|_| "获取失败 (请检查局域网)".to_string());

        let default_port = 21118;
        let stop_flag = Arc::new(AtomicBool::new(false));
        let host_status = Arc::new(Mutex::new(format!("正在启动监听端口 {}...", default_port)));

        // 启动默认被控端监听
        let sf_clone = stop_flag.clone();
        let st_clone = host_status.clone();
        rt_handle.spawn(async move {
            run_host_server(default_port, sf_clone, st_clone).await;
        });

        Self {
            local_ip_str: ip,
            listen_port_input: default_port.to_string(),
            current_listen_port: default_port,
            host_stop_flag: stop_flag,
            host_status,
            target_addr_input: String::new(),
            client_status: Arc::new(Mutex::new("未连接".to_string())),
            is_connected: Arc::new(AtomicBool::new(false)),
            latest_frame: Arc::new(Mutex::new(None)),
            remote_texture: None,
            input_tx: None,
            is_fullscreen: false,
            rt_handle,
        }
    }

    fn restart_host_listener(&mut self) {
        if let Ok(new_port) = self.listen_port_input.trim().parse::<u16>() {
            if new_port != self.current_listen_port && new_port > 0 {
                self.host_stop_flag.store(true, Ordering::Relaxed);
                let new_stop = Arc::new(AtomicBool::new(false));
                self.host_stop_flag = new_stop.clone();
                self.current_listen_port = new_port;

                let st_clone = self.host_status.clone();
                self.rt_handle.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    run_host_server(new_port, new_stop, st_clone).await;
                });
            }
        } else {
            *self.host_status.lock().unwrap() = "无效的端口号".to_string();
        }
    }

    fn connect_to_remote(&mut self, ctx: egui::Context) {
        let mut target = self.target_addr_input.trim().to_string();
        if target.is_empty() {
            return;
        }
        // 如果用户只输入了 IP，自动补全默认端口 21118
        if !target.contains(':') {
            target = format!("{}:21118", target);
            self.target_addr_input = target.clone();
        }

        let (tx, mut rx) = mpsc::unbounded_channel::<NetPacket>();
        self.input_tx = Some(tx);

        let is_connected = self.is_connected.clone();
        let client_status = self.client_status.clone();
        let latest_frame = self.latest_frame.clone();

        *client_status.lock().unwrap() = format!("正在连接 {}...", target);

        self.rt_handle.spawn(async move {
            match TcpStream::connect(&target).await {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    is_connected.store(true, Ordering::Relaxed);
                    *client_status.lock().unwrap() = format!("已连接到 {}", target);

                    let (mut reader, mut writer) = stream.into_split();

                    // 发送键鼠事件任务
                    let conn_flag_tx = is_connected.clone();
                    let send_handle = tokio::spawn(async move {
                        while let Some(pkt) = rx.recv().await {
                            if !conn_flag_tx.load(Ordering::Relaxed) {
                                break;
                            }
                            if send_packet(&mut writer, &pkt).await.is_err() {
                                break;
                            }
                        }
                        conn_flag_tx.store(false, Ordering::Relaxed);
                    });

                    // 接收屏幕帧任务
                    let conn_flag_rx = is_connected.clone();
                    while conn_flag_rx.load(Ordering::Relaxed) {
                        match recv_packet(&mut reader).await {
                            Ok(NetPacket::ScreenFrame { jpeg_data, .. }) => {
                                if let Ok(img) = image::load_from_memory(&jpeg_data) {
                                    let rgba = img.to_rgba8();
                                    let w = rgba.width() as usize;
                                    let h = rgba.height() as usize;
                                    *latest_frame.lock().unwrap() = Some((rgba.into_raw(), w, h));
                                    ctx.request_repaint();
                                }
                            }
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }

                    conn_flag_rx.store(false, Ordering::Relaxed);
                    send_handle.abort();
                    *client_status.lock().unwrap() = "连接已断开".to_string();
                    ctx.request_repaint();
                }
                Err(e) => {
                    *client_status.lock().unwrap() = format!("连接失败: {}", e);
                    ctx.request_repaint();
                }
            }
        });
    }

    fn disconnect(&mut self) {
        self.is_connected.store(false, Ordering::Relaxed);
        self.input_tx = None;
        *self.latest_frame.lock().unwrap() = None;
        self.remote_texture = None;
        *self.client_status.lock().unwrap() = "已主动断开连接".to_string();
    }
}

impl eframe::App for RemoteDesktopApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let connected = self.is_connected.load(Ordering::Relaxed);

        // ==========================================
        // 顶部控制栏 (本机IP、端口设置、连接输入框、全屏/最小化按钮)
        // ==========================================
        egui::TopBottomPanel::top("top_control_bar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                // 1. 本机信息与自定义端口
                ui.label(egui::RichText::new("本机 IP:").strong());
                ui.label(
                    egui::RichText::new(&self.local_ip_str)
                        .color(egui::Color32::from_rgb(0, 180, 100))
                        .strong(),
                );
                if ui.button("📋 复制").clicked() {
                    ui.output_mut(|o| {
                        o.copied_text = format!("{}:{}", self.local_ip_str, self.current_listen_port);
                    });
                }

                ui.separator();
                ui.label("监听端口:");
                ui.add(egui::TextEdit::singleline(&mut self.listen_port_input).desired_width(55.0));
                if ui.button("应用").clicked() {
                    self.restart_host_listener();
                }

                ui.separator();

                // 2. 远程连接输入与按钮
                ui.label("目标地址:");
                let input_resp = ui.add_enabled(
                    !connected,
                    egui::TextEdit::singleline(&mut self.target_addr_input)
                        .hint_text("192.168.1.2:21118")
                        .desired_width(145.0),
                );
                if input_resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) && !connected {
                    self.connect_to_remote(ctx.clone());
                }

                if !connected {
                    if ui.button(egui::RichText::new("🔗 连接").strong()).clicked() {
                        self.connect_to_remote(ctx.clone());
                    }
                } else if ui.button(egui::RichText::new("❌ 断开").color(egui::Color32::RED)).clicked() {
                    self.disconnect();
                }

                // 3. 右侧窗口控制按钮：最小化 & 全屏
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let fs_text = if self.is_fullscreen { "🗗 退出全屏" } else { "⛶ 全屏" };
                    if ui.button(fs_text).clicked() {
                        self.is_fullscreen = !self.is_fullscreen;
                        ctx.send_viewport_cmd(egui::ViewportCommand::Fullscreen(self.is_fullscreen));
                    }

                    if ui.button("🗕 最小化").clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
                    }
                });
            });
            ui.add_space(4.0);
        });

        // ==========================================
        // 主视图区域
        // ==========================================
        egui::CentralPanel::default()
            .frame(egui::Frame::none().fill(egui::Color32::from_rgb(24, 24, 28)))
            .show(ctx, |ui| {
                if connected {
                    // 更新最新帧纹理
                    if let Some((rgba, w, h)) = self.latest_frame.lock().unwrap().take() {
                        let color_image = egui::ColorImage::from_rgba_unmultiplied([w, h], &rgba);
                        self.remote_texture = Some(ctx.load_texture(
                            "remote_screen",
                            color_image,
                            egui::TextureOptions::LINEAR,
                        ));
                    }

                    if let Some(tex) = &self.remote_texture {
                        let avail = ui.available_size();
                        let tex_size = tex.size_vec2();
                        let scale = (avail.x / tex_size.x).min(avail.y / tex_size.y);
                        let render_size = tex_size * scale;

                        // 居中绘制远程桌面画面
                        let offset = (avail - render_size) * 0.5;
                        let rect = egui::Rect::from_min_size(ui.min_rect().min + offset, render_size);

                        let response = ui.allocate_rect(rect, egui::Sense::click_and_drag());
                        ui.painter().image(
                            tex.id(),
                            rect,
                            egui::Rect::from_min_max(egui::pos2(0.0, 0.0), egui::pos2(1.0, 1.0)),
                            egui::Color32::WHITE,
                        );

                        // 捕获鼠标移动与点击并发送至远端
                        if let Some(tx) = &self.input_tx {
                            if let Some(pos) = response.hover_pos() {
                                if rect.contains(pos) {
                                    let x_ratio = (pos.x - rect.min.x) / rect.width();
                                    let y_ratio = (pos.y - rect.min.y) / rect.height();
                                    let _ = tx.send(NetPacket::MouseMove { x_ratio, y_ratio });
                                }
                            }

                            ctx.input(|i| {
                                // 鼠标按键
                                for (egui_btn, net_btn) in [
                                    (egui::PointerButton::Primary, MouseBtn::Left),
                                    (egui::PointerButton::Secondary, MouseBtn::Right),
                                    (egui::PointerButton::Middle, MouseBtn::Middle),
                                ] {
                                    if i.pointer.button_pressed(egui_btn) && response.hovered() {
                                        let _ = tx.send(NetPacket::MouseButton { button: net_btn, down: true });
                                    }
                                    if i.pointer.button_released(egui_btn) {
                                        let _ = tx.send(NetPacket::MouseButton { button: net_btn, down: false });
                                    }
                                }

                                // 鼠标滚轮
                                if response.hovered() && i.raw_scroll_delta.y != 0.0 {
                                    let delta = if i.raw_scroll_delta.y > 0.0 { 1 } else { -1 };
                                    let _ = tx.send(NetPacket::MouseScroll { delta_y: delta });
                                }

                                // 键盘事件转发
                                for event in &i.events {
                                    match event {
                                        egui::Event::Text(text) => {
                                            for ch in text.chars() {
                                                let _ = tx.send(NetPacket::KeyInput {
                                                    key_char: Some(ch),
                                                    special_key: None,
                                                    down: true,
                                                });
                                                let _ = tx.send(NetPacket::KeyInput {
                                                    key_char: Some(ch),
                                                    special_key: None,
                                                    down: false,
                                                });
                                            }
                                        }
                                        egui::Event::Key { key, pressed, .. } => {
                                            let sp = match key {
                                                egui::Key::Enter => Some(SpecialKey::Enter),
                                                egui::Key::Backspace => Some(SpecialKey::Backspace),
                                                egui::Key::Tab => Some(SpecialKey::Tab),
                                                egui::Key::Escape => Some(SpecialKey::Escape),
                                                egui::Key::Space => Some(SpecialKey::Space),
                                                egui::Key::ArrowUp => Some(SpecialKey::Up),
                                                egui::Key::ArrowDown => Some(SpecialKey::Down),
                                                egui::Key::ArrowLeft => Some(SpecialKey::Left),
                                                egui::Key::ArrowRight => Some(SpecialKey::Right),
                                                egui::Key::Delete => Some(SpecialKey::Delete),
                                                _ => None,
                                            };
                                            if let Some(sk) = sp {
                                                let _ = tx.send(NetPacket::KeyInput {
                                                    key_char: None,
                                                    special_key: Some(sk),
                                                    down: *pressed,
                                                });
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            });
                        }
                    } else {
                        ui.centered_and_justified(|ui| {
                            ui.label(egui::RichText::new("正在等待对方屏幕画面传输...").size(18.0));
                        });
                    }
                } else {
                    // 未连接时的状态面板
                    ui.centered_and_justified(|ui| {
                        ui.vertical_centered(|ui| {
                            ui.add_space(120.0);
                            ui.heading("局域网轻量远程桌面");
                            ui.add_space(16.0);
                            ui.label(format!(
                                "本机受控地址: {}:{}",
                                self.local_ip_str, self.current_listen_port
                            ));
                            ui.label(format!("受控端状态: {}", *self.host_status.lock().unwrap()));
                            ui.add_space(8.0);
                            ui.label(format!("主控端状态: {}", *self.client_status.lock().unwrap()));
                            ui.add_space(20.0);
                            ui.label(
                                egui::RichText::new("提示：将上方本机 IP 和端口告知对方，或在顶部输入对方 IP:端口 进行控制")
                                    .color(egui::Color32::GRAY),
                            );
                        });
                    });
                }
            });
    }
}

fn main() -> eframe::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    let rt_handle = rt.handle().clone();

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1024.0, 680.0])
            .with_title("LAN Remote Desktop (Rust)"),
        ..Default::default()
    };

    eframe::run_native(
        "LAN Remote Desktop",
        native_options,
        Box::new(move |cc| Ok(Box::new(RemoteDesktopApp::new(cc, rt_handle)))),
    )
}
