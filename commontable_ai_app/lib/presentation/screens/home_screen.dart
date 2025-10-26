import 'package:flutter/material.dart';
import 'package:commontable_ai_app/routes/app_routes.dart';

class HomeScreen extends StatelessWidget {
  const HomeScreen({super.key});

  @override
  Widget build(BuildContext context) {
    final List<_FeatureCard> features = [
      _FeatureCard(
        title: 'AI Food Scanner',
        description:
            'Snap a picture of your meal and get instant nutrition info.',
        icon: Icons.camera_alt,
        color: Colors.green.shade400,
        route: AppRoutes.nutritionAnalysis,
      ),
      _FeatureCard(
        title: 'Meal Plans',
        description: 'Personalized nutrition plans tailored to your goals.',
        icon: Icons.restaurant_menu,
        color: Colors.orange.shade400,
        route: AppRoutes.mealPlans,
      ),
      _FeatureCard(
        title: 'AI Coach',
        description:
            'Chat with your personal nutrition assistant for guidance.',
        icon: Icons.chat_bubble_outline,
        color: Colors.blue.shade400,
        route: AppRoutes.chatbot,
      ),
      _FeatureCard(
        title: 'Progress Tracker',
        description: 'Visualize your nutrition and health journey over time.',
        icon: Icons.show_chart,
        color: Colors.purple.shade400,
        route: AppRoutes.progress,
      ),
    ];

    return Scaffold(
      appBar: AppBar(
        title: const Text(
          'Commontable AI',
          style: TextStyle(fontWeight: FontWeight.bold),
        ),
        centerTitle: true,
        backgroundColor: Colors.green,
        foregroundColor: Colors.white,
        elevation: 0,
      ),
      body: Container(
        color: Colors.grey.shade100,
        child: ListView(
          padding: const EdgeInsets.all(16),
          children: [
            const _WelcomeHeader(),
            const SizedBox(height: 20),
            GridView.builder(
              shrinkWrap: true,
              physics: const NeverScrollableScrollPhysics(),
              gridDelegate: const SliverGridDelegateWithFixedCrossAxisCount(
                crossAxisCount: 2,
                crossAxisSpacing: 12,
                mainAxisSpacing: 12,
                childAspectRatio: 0.95,
              ),
              itemCount: features.length,
              itemBuilder: (context, index) {
                final feature = features[index];
                return GestureDetector(
                  onTap: () => Navigator.pushNamed(context, feature.route),
                  child: _FeatureCardWidget(feature: feature),
                );
              },
            ),
          ],
        ),
      ),
      drawer: const _AppDrawer(),
    );
  }
}

// 🟢 WELCOME HEADER
class _WelcomeHeader extends StatelessWidget {
  const _WelcomeHeader();

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(18),
      decoration: BoxDecoration(
        color: Colors.green.shade400,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: const [
          Text(
            'Welcome Back 👋',
            style: TextStyle(color: Colors.white, fontSize: 20),
          ),
          SizedBox(height: 8),
          Text(
            'Your AI nutrition companion is ready to help you make smarter food choices today!',
            style: TextStyle(color: Colors.white70, fontSize: 14),
          ),
        ],
      ),
    );
  }
}

// 🟢 FEATURE CARD
class _FeatureCard {
  final String title;
  final String description;
  final IconData icon;
  final Color color;
  final String route;

  _FeatureCard({
    required this.title,
    required this.description,
    required this.icon,
    required this.color,
    required this.route,
  });
}

class _FeatureCardWidget extends StatelessWidget {
  final _FeatureCard feature;

  const _FeatureCardWidget({required this.feature});

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(
        color: feature.color,
        borderRadius: BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(
            color: feature.color.withOpacity(0.3),
            blurRadius: 8,
            offset: const Offset(0, 3),
          ),
        ],
      ),
      padding: const EdgeInsets.all(16),
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(feature.icon, color: Colors.white, size: 40),
          const SizedBox(height: 10),
          Text(
            feature.title,
            textAlign: TextAlign.center,
            style: const TextStyle(
              color: Colors.white,
              fontWeight: FontWeight.bold,
              fontSize: 16,
            ),
          ),
          const SizedBox(height: 8),
          Text(
            feature.description,
            textAlign: TextAlign.center,
            style: const TextStyle(color: Colors.white70, fontSize: 13),
          ),
        ],
      ),
    );
  }
}

// 🟢 NAVIGATION DRAWER
class _AppDrawer extends StatelessWidget {
  const _AppDrawer();

  @override
  Widget build(BuildContext context) {
    return Drawer(
      child: ListView(
        children: [
          const DrawerHeader(
            decoration: BoxDecoration(color: Colors.green),
            child: Text(
              'Commontable AI Menu',
              style: TextStyle(color: Colors.white, fontSize: 20),
            ),
          ),
          ListTile(
            leading: const Icon(Icons.settings),
            title: const Text('Settings'),
            onTap: () => Navigator.pushNamed(context, AppRoutes.settings),
          ),
          ListTile(
            leading: const Icon(Icons.info_outline),
            title: const Text('About'),
            onTap: () {
              showAboutDialog(
                context: context,
                applicationName: 'Commontable AI',
                applicationVersion: '1.0.0',
                children: const [
                  Text(
                    'An AI-powered nutrition recommendation system built with Flutter.',
                  ),
                ],
              );
            },
          ),
        ],
      ),
    );
  }
}
